package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	ModifyKind = "elasticache-modify"
)

type ModifyArgs struct {
	Instance *RedisInstance `json:"instance"`
}

func (ModifyArgs) Kind() string { return ModifyKind }

type ModifyWorker struct {
	river.WorkerDefaults[ModifyArgs]
	db          *gorm.DB
	settings    *config.Settings
	elasticache ElasticacheClientInterface
	logger      *slog.Logger
}

func NewModifyWorker(
	db *gorm.DB,
	settings *config.Settings,
	elasticache ElasticacheClientInterface,
	logger *slog.Logger,
) *ModifyWorker {
	return &ModifyWorker{
		db:          db,
		settings:    settings,
		elasticache: elasticache,
		logger:      logger,
	}
}

func (w *ModifyWorker) Work(ctx context.Context, job *river.Job[ModifyArgs]) error {
	return w.asyncModifyRedis(ctx, job.Args.Instance)
}

func (w *ModifyWorker) asyncModifyRedis(ctx context.Context, i *RedisInstance) error {
	operation := base.ModifyOp

	params, err := prepareModifyReplicationGroupInput(i)
	if err != nil {
		w.logger.Error("error preparing modify replication group input", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error preparing modify input: %s", err))
		return river.JobCancel(fmt.Errorf("asyncModifyRedis: error preparing modify input %w ", err))
	}

	if i.NewReplicaCount > 0 {
		err = w.increaseReplicaCount(ctx, i, operation)
		if err != nil {
			w.logger.Error("error increasing replica count", "err", err)
			asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("error increasing replica count: %s", err))
			return river.JobCancel(fmt.Errorf("asyncModifyRedis: error increasing replica count %w ", err))
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Modifying replication group")

	_, err = w.elasticache.ModifyReplicationGroup(ctx, params)
	if err != nil {
		w.logger.Error("error modifying replication group", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying cluster: %s", err))
		return river.JobCancel(fmt.Errorf("asyncModifyRedis: error modifying replication group %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished modifying cluster")
	return nil
}

func (w *ModifyWorker) increaseReplicaCount(ctx context.Context, i *RedisInstance, operation base.Operation) error {
	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Adding new replica nodes")

	newReplicaCount, err := common.ConvertIntToInt32Safely(i.NewReplicaCount)
	if err != nil {
		return err
	}

	_, err = w.elasticache.IncreaseReplicaCount(ctx, &elasticache.IncreaseReplicaCountInput{
		ReplicationGroupId: &i.ClusterID,
		NewReplicaCount:    newReplicaCount,
		ApplyImmediately:   aws.Bool(true),
	})
	if err != nil {
		w.logger.Error("error increasing replica count", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error increasing replica count: %s", err))
		return err
	}

	// Wait for replication group to be available
	waiter := elasticache.NewReplicationGroupAvailableWaiter(w.elasticache, func(dawo *elasticache.ReplicationGroupAvailableWaiterOptions) {
		dawo.MinDelay = w.settings.PollAwsMinDelay
	})

	waiterInput := &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	}

	err = waiter.Wait(ctx, waiterInput, w.settings.PollAwsMaxDuration)
	if err != nil {
		return err
	}

	err = w.verifyIncreasedReplicaCount(ctx, i)
	if err != nil {
		return err
	}

	return nil
}

func (w *ModifyWorker) verifyIncreasedReplicaCount(ctx context.Context, i *RedisInstance) error {
	var nodesReady bool

	attempts := 1
	maxAttempts := 1 + int(w.settings.PollAwsMaxRetries)

	for !nodesReady && attempts <= maxAttempts {
		w.logger.Info(fmt.Sprintf("verifying replica creation. attempt %d of %d", attempts, maxAttempts))
		output, err := w.elasticache.DescribeReplicationGroups(ctx, &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: &i.ClusterID,
		})

		if err != nil {
			return err
		}

		nodeGroup := output.ReplicationGroups[0].NodeGroups[0]
		status := *nodeGroup.Status

		var replicaNodes []elasticacheTypes.NodeGroupMember
		for _, nodeMember := range nodeGroup.NodeGroupMembers {
			if *nodeMember.CurrentRole == "replica" {
				replicaNodes = append(replicaNodes, nodeMember)
			}
		}

		nodesReady = (status == "available" && len(replicaNodes) == i.NewReplicaCount)
		if nodesReady {
			break
		}

		attempts += 1
		time.Sleep(w.settings.PollAwsMinDelay)
		continue
	}

	return nil
}

func prepareModifyReplicationGroupInput(i *RedisInstance) (*elasticache.ModifyReplicationGroupInput, error) {
	securityGroups := []string{i.SecGroup}

	snapshotRetentionLimit, err := common.ConvertIntToInt32Safely(i.SnapshotRetentionLimit)
	if err != nil {
		return nil, err
	}

	// Standard parameters
	params := &elasticache.ModifyReplicationGroupInput{
		ReplicationGroupDescription: aws.String(i.Description),
		AutomaticFailoverEnabled:    aws.Bool(i.AutomaticFailoverEnabled),
		ReplicationGroupId:          aws.String(i.ClusterID),
		CacheNodeType:               aws.String(i.CacheNodeType),
		SecurityGroupIds:            securityGroups,
		Engine:                      aws.String(i.Engine),
		PreferredMaintenanceWindow:  aws.String(i.PreferredMaintenanceWindow),
		SnapshotWindow:              aws.String(i.SnapshotWindow),
		SnapshotRetentionLimit:      snapshotRetentionLimit,
	}
	if i.EngineVersion != "" {
		params.EngineVersion = aws.String(i.EngineVersion)
	}
	return params, nil
}
