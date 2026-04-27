package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	DeleteKind = "elasticache-delete"
)

type DeleteArgs struct {
	Instance *RedisInstance `json:"instance"`
}

func (DeleteArgs) Kind() string { return DeleteKind }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	db          *gorm.DB
	settings    *config.Settings
	elasticache ElasticacheClientInterface
	s3          brokerAws.S3ClientInterface
	logger      *slog.Logger
}

func NewDeleteWorker(
	db *gorm.DB,
	settings *config.Settings,
	elasticache ElasticacheClientInterface,
	s3 brokerAws.S3ClientInterface,
	logger *slog.Logger,
) *DeleteWorker {
	return &DeleteWorker{
		db:          db,
		settings:    settings,
		elasticache: elasticache,
		s3:          s3,
		logger:      logger,
	}
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return w.asyncDeleteRedis(ctx, job.Args.Instance)
}

func (w *DeleteWorker) asyncDeleteRedis(ctx context.Context, i *RedisInstance) error {
	operation := base.DeleteOp

	asyncmessage.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting replication group")

	params := &elasticache.DeleteReplicationGroupInput{
		ReplicationGroupId:      aws.String(i.ClusterID), // Required
		FinalSnapshotIdentifier: aws.String(i.ClusterID + "-final"),
	}
	_, err := w.elasticache.DeleteReplicationGroup(ctx, params)

	if err != nil {
		w.logger.Error("asyncDeleteRedis: DeleteReplicationGroup failed", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("asyncDeleteRedis: DeleteReplicationGroup failed: %s", err))
		return river.JobCancel(fmt.Errorf("asyncModifyRedis: error preparing delete replication group input %w ", err))
	}

	// Create a waiter
	waiter := elasticache.NewReplicationGroupDeletedWaiter(w.elasticache, func(dawo *elasticache.ReplicationGroupDeletedWaiterOptions) {
		dawo.MinDelay = w.settings.PollAwsMinDelay
	})
	waiterInput := &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	}
	err = waiter.Wait(ctx, waiterInput, w.settings.PollAwsMaxDuration)
	if err != nil {
		w.logger.Error("error waiting for cluster to be deleted", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Error waiting for cluster to be deleted: %s", err))
		return river.JobCancel(fmt.Errorf("asyncModifyRedis: error waiting for cluster to be deleted %w ", err))
	}

	asyncmessage.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Exporting snapshot")

	err = w.exportRedisSnapshot(ctx, i)
	if err != nil {
		w.logger.Error("asyncDeleteRedis: exportRedisSnapshot failed", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("asyncDeleteRedis: exportRedisSnapshot failed: %s", err))
		return river.JobCancel(fmt.Errorf("asyncModifyRedis: error exporting snapshot %w ", err))
	}

	err = w.db.Unscoped().Delete(i).Error
	if err != nil {
		w.logger.Error("asyncDeleteRedis: error deleting record", "err", err)
		return river.JobCancel(fmt.Errorf("asyncModifyRedis: deleting record %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Finished deleting replication group")
	return nil
}

func (w *DeleteWorker) exportRedisSnapshot(ctx context.Context, i *RedisInstance) error {
	path := i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
	bucket := w.settings.SnapshotsBucketName

	snapshot_name := i.ClusterID + "-final"
	sleep := 30 * time.Second
	w.logger.Info("exportRedisSnapshot: Waiting for Instance Snapshot to Complete")

	// poll for snapshot being available
	check_input := &elasticache.DescribeSnapshotsInput{
		SnapshotName: &snapshot_name,
	}
	for {
		resp, err := w.elasticache.DescribeSnapshots(ctx, check_input)
		if err != nil {
			w.logger.Error("exportRedisSnapshot: Redis.DescribeSnapshots Failed", "err", err)
			return err
		}

		if *(resp.Snapshots[0].SnapshotStatus) == "available" {
			break
		}
		time.Sleep(sleep)
	}

	w.logger.Info("exportRedisSnapshot: Exporting Instance Snapshot to s3")
	// export to s3 bucket so copy will autoexpire after 14 days
	copy_input := &elasticache.CopySnapshotInput{
		TargetBucket:       aws.String(bucket),
		TargetSnapshotName: aws.String(path + "/" + snapshot_name),
		SourceSnapshotName: aws.String(snapshot_name),
	}
	_, err := w.elasticache.CopySnapshot(ctx, copy_input)
	if err != nil {
		w.logger.Error("exportRedisSnapshot: Redis.CopySnapshot Failed", "err", err)
		return err
	}

	w.logger.Info("exportRedisSnapshot: Writing Instance manifest to s3")
	// write instance to manifest
	// marshall instance to bytes.
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)

	serverSideEncryption, err := brokerAws.GetS3ServerSideEncryptionEnum("AES256")
	if err != nil {
		w.logger.Error("exportRedisSnapshot: GetS3ServerSideEncryptionEnum failed", "err", err)
		return err
	}

	input := s3.PutObjectInput{
		Body:                 body,
		Bucket:               aws.String(bucket),
		Key:                  aws.String(path + "/instance_manifest.json"),
		ServerSideEncryption: *serverSideEncryption,
	}

	// drop info to s3
	_, err = w.s3.PutObject(ctx, &input)
	// Decide if AWS service call was successful
	if err != nil {
		w.logger.Error("exportRedisSnapshot: S3.PutObject Failed", "err", err)
		return err
	}

	w.logger.Info("exportRedisSnapshot: Waiting for Instance Snapshot Copy to Complete")
	// poll for snapshot being available again before delete
	check_input = &elasticache.DescribeSnapshotsInput{
		SnapshotName: &snapshot_name,
	}
	for {
		resp, err := w.elasticache.DescribeSnapshots(ctx, check_input)
		if err != nil {
			w.logger.Error("exportRedisSnapshot: Redis.DescribeSnapshots Failed", "err", err)
			return err
		}

		if *(resp.Snapshots[0].SnapshotStatus) == "available" {
			break
		}
		time.Sleep(sleep)
	}

	w.logger.Info("exportRedisSnapshot: Deleting ElatiCache Service Snapshot", "err", err)
	// now cleanup snapshot from ElastiCache
	delete_input := &elasticache.DeleteSnapshotInput{
		SnapshotName: aws.String(snapshot_name),
	}
	_, err = w.elasticache.DeleteSnapshot(ctx, delete_input)
	if err != nil {
		w.logger.Error("Redis.DeleteSnapshot: Failed", "err", err)
		return err
	}

	w.logger.Info("exportRedisSnapshot: Snapshot and Manifest backup to s3 Complete")
	return nil
}
