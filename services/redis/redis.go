package redis

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"code.cloudfoundry.org/lager"

	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"bytes"
	"fmt"
)

type redisAdapter interface {
	createRedis(i *RedisInstance, password string) (base.InstanceState, error)
	modifyRedis(i *RedisInstance, password string) (base.InstanceState, error)
	checkRedisStatus(i *RedisInstance) (base.InstanceState, error)
	bindRedisToApp(i *RedisInstance, password string) (map[string]string, error)
	deleteRedis(i *RedisInstance) (base.InstanceState, error)
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(s *config.Settings, logger lager.Logger) (redisAdapter, error) {
	var redisAdapter redisAdapter

	if s.Environment == "test" {
		redisAdapter = &mockRedisAdapter{}
		return redisAdapter, nil
	}

	cfg, err := awsConfig.LoadDefaultConfig(
		context.TODO(),
		awsConfig.WithRegion(s.Region),
	)
	if err != nil {
		return nil, err
	}

	elasticacheClient := elasticache.NewFromConfig(cfg)
	s3 := s3.NewFromConfig(cfg)

	redisAdapter = &dedicatedRedisAdapter{
		settings:    *s,
		logger:      logger,
		elasticache: elasticacheClient,
		s3:          s3,
	}
	return redisAdapter, nil
}

type mockRedisAdapter struct {
}

func (d *mockRedisAdapter) createRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

func (d *mockRedisAdapter) modifyRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	return base.InstanceNotModified, nil
}

func (d *mockRedisAdapter) checkRedisStatus(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *mockRedisAdapter) bindRedisToApp(i *RedisInstance, password string) (map[string]string, error) {
	return i.getCredentials(password)
}

func (d *mockRedisAdapter) deleteRedis(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceGone, nil
}

type dedicatedRedisAdapter struct {
	settings    config.Settings
	logger      lager.Logger
	elasticache ElasticacheClientInterface
	s3          brokerAws.S3ClientInterface
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-redis-broker-"

func (d *dedicatedRedisAdapter) createRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	// Standard parameters
	params, err := prepareCreateReplicationGroupInput(i, password)
	if err != nil {
		d.logger.Error("prepareCreateReplicationGroupInput err", err)
		return base.InstanceNotCreated, err
	}

	_, err = d.elasticache.CreateReplicationGroup(context.TODO(), params)
	if err != nil {
		d.logger.Error("CreateReplicationGroup err", err)
		return base.InstanceNotCreated, err
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedRedisAdapter) modifyRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	return base.InstanceNotModified, nil
}

func (d *dedicatedRedisAdapter) checkRedisStatus(i *RedisInstance) (base.InstanceState, error) {
	// First, we need to check if the instance state
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String(i.ClusterID), // Required
		}

		resp, err := d.elasticache.DescribeReplicationGroups(context.TODO(), params)
		if err != nil {
			d.logger.Error("checkRedisStatus: DescribeReplicationGroups failed", err)
			return base.InstanceNotCreated, err
		}

		numOfInstances := len(resp.ReplicationGroups)
		if numOfInstances > 0 {
			for _, value := range resp.ReplicationGroups {
				d.logger.Debug(fmt.Sprintf("Redis Instance: %s is %s", i.ClusterID, *(value.Status)))
				switch *(value.Status) {
				case "available":
					return base.InstanceReady, nil
				case "creating":
					return base.InstanceInProgress, nil
				case "create-failed":
					return base.InstanceNotCreated, nil
				case "deleting":
					return base.InstanceNotGone, nil
				default:
					return base.InstanceInProgress, nil
				}

			}
		} else {
			return base.InstanceNotCreated, errors.New("couldn't find any instances")
		}
	}

	return base.InstanceNotCreated, nil
}

func (d *dedicatedRedisAdapter) bindRedisToApp(i *RedisInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String(i.ClusterID), // Required
		}

		resp, err := d.elasticache.DescribeReplicationGroups(context.TODO(), params)
		if err != nil {
			d.logger.Error("bindRedisToApp: DescribeReplicationGroups failed", err)
			return nil, err
		}

		numOfInstances := len(resp.ReplicationGroups)
		if numOfInstances > 0 {
			for _, value := range resp.ReplicationGroups {
				// First check that the instance is up.
				if value.Status != nil && *(value.Status) == "available" {
					if value.NodeGroups[0].PrimaryEndpoint != nil && value.NodeGroups[0].PrimaryEndpoint.Address != nil && value.NodeGroups[0].PrimaryEndpoint.Port != nil {
						port := *(value.NodeGroups[0].PrimaryEndpoint.Port)
						d.logger.Debug(fmt.Sprintf("host: %s port: %d \n", *(value.NodeGroups[0].PrimaryEndpoint.Address), port))

						i.Port = int64(port)
						i.Host = *(value.NodeGroups[0].PrimaryEndpoint.Address)
						i.State = base.InstanceReady
						// Should only be one regardless. Just return now.
						break
					} else {
						// Something went horribly wrong. Should never get here.
						return nil, errors.New("invalid memory for endpoint and/or endpoint members")
					}
				} else {
					// Instance not up yet.
					return nil, errors.New("instance not available yet. Please wait and try again")
				}
			}
		}
	}
	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(password)
}

func (d *dedicatedRedisAdapter) deleteRedis(i *RedisInstance) (base.InstanceState, error) {
	params := &elasticache.DeleteReplicationGroupInput{
		ReplicationGroupId:      aws.String(i.ClusterID), // Required
		FinalSnapshotIdentifier: aws.String(i.ClusterID + "-final"),
	}
	_, err := d.elasticache.DeleteReplicationGroup(context.TODO(), params)

	if err != nil {
		d.logger.Error("deleteRedis: DeleteReplicationGroup failed", err)
		return base.InstanceNotGone, err
	}

	go d.exportRedisSnapshot(i)
	return base.InstanceGone, nil
}

func (d *dedicatedRedisAdapter) exportRedisSnapshot(i *RedisInstance) {
	path := i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
	bucket := d.settings.SnapshotsBucketName

	snapshot_name := i.ClusterID + "-final"
	sleep := 30 * time.Second
	d.logger.Info("exportRedisSnapshot: Waiting for Instance Snapshot to Complete", lager.Data{"uuid": i.Uuid})

	// poll for snapshot being available
	check_input := &elasticache.DescribeSnapshotsInput{
		SnapshotName: &snapshot_name,
	}
	for {
		resp, err := d.elasticache.DescribeSnapshots(context.TODO(), check_input)
		if err != nil {
			d.logger.Error("exportRedisSnapshot: Redis.DescribeSnapshots Failed", err, lager.Data{"uuid": i.Uuid})
			return
		}

		if *(resp.Snapshots[0].SnapshotStatus) == "available" {
			break
		}
		time.Sleep(sleep)
	}

	d.logger.Info("exportRedisSnapshot: Exporting Instance Snapshot to s3", lager.Data{"uuid": i.Uuid})
	// export to s3 bucket so copy will autoexpire after 14 days
	copy_input := &elasticache.CopySnapshotInput{
		TargetBucket:       aws.String(bucket),
		TargetSnapshotName: aws.String(path + "/" + snapshot_name),
		SourceSnapshotName: aws.String(snapshot_name),
	}
	_, err := d.elasticache.CopySnapshot(context.TODO(), copy_input)
	if err != nil {
		d.logger.Error("exportRedisSnapshot: Redis.CopySnapshot Failed", err, lager.Data{"uuid": i.Uuid})
		return
	}

	d.logger.Info("exportRedisSnapshot: Writing Instance manisfest to s3", lager.Data{"uuid": i.Uuid})
	// write instance to manifest
	// marshall instance to bytes.
	data, err := json.Marshal(i)
	if err != nil {
		return
	}
	body := bytes.NewReader(data)

	serverSideEncryption, err := brokerAws.GetS3ServerSideEncryptionEnum("AES256")
	if err != nil {
		d.logger.Error("exportRedisSnapshot: GetS3ServerSideEncryptionEnum failed", err)
		return
	}

	input := s3.PutObjectInput{
		Body:                 body,
		Bucket:               aws.String(bucket),
		Key:                  aws.String(path + "/instance_manifest.json"),
		ServerSideEncryption: *serverSideEncryption,
	}

	// drop info to s3
	_, err = d.s3.PutObject(context.TODO(), &input)
	// Decide if AWS service call was successful
	if err != nil {
		d.logger.Error("exportRedisSnapshot: S3.PutObject Failed", err, lager.Data{"uuid": i.Uuid})
		return
	}

	d.logger.Info("exportRedisSnapshot: Waiting for Instance Snapshot Copy to Complete", lager.Data{"uuid": i.Uuid})
	// poll for snapshot being available again before delete
	check_input = &elasticache.DescribeSnapshotsInput{
		SnapshotName: &snapshot_name,
	}
	for {
		resp, err := d.elasticache.DescribeSnapshots(context.TODO(), check_input)
		if err != nil {
			d.logger.Error("exportRedisSnapshot: Redis.DescribeSnapshots Failed", err, lager.Data{"uuid": i.Uuid})
			return
		}

		if *(resp.Snapshots[0].SnapshotStatus) == "available" {
			break
		}
		time.Sleep(sleep)
	}

	d.logger.Info("exportRedisSnapshot: Deleting ElatiCache Service Snapshot", lager.Data{"uuid": i.Uuid})
	// now cleanup snapshot from ElastiCache
	delete_input := &elasticache.DeleteSnapshotInput{
		SnapshotName: aws.String(snapshot_name),
	}
	_, err = d.elasticache.DeleteSnapshot(context.TODO(), delete_input)
	if err != nil {
		d.logger.Error("Redis.DeleteSnapshot: Failed", err, lager.Data{"uuid": i.Uuid})
		return
	}

	d.logger.Info("exportRedisSnapshot: Snapshot and Manifest backup to s3 Complete.", lager.Data{"uuid": i.Uuid})
}

func prepareCreateReplicationGroupInput(
	i *RedisInstance,
	password string,
) (*elasticache.CreateReplicationGroupInput, error) {
	redisTags := ConvertTagsToElasticacheTags(i.Tags)

	var securityGroups []string
	securityGroups = append(securityGroups, i.SecGroup)

	numCacheClusters, err := common.ConvertIntToInt32Safely(i.NumCacheClusters)
	if err != nil {
		return nil, err
	}

	snapshotRetentionLimit, err := common.ConvertIntToInt32Safely(i.SnapshotRetentionLimit)
	if err != nil {
		return nil, err
	}

	// Standard parameters
	params := &elasticache.CreateReplicationGroupInput{
		AtRestEncryptionEnabled:     aws.Bool(true),
		TransitEncryptionEnabled:    aws.Bool(true),
		AutoMinorVersionUpgrade:     aws.Bool(true),
		ReplicationGroupDescription: aws.String(i.Description),
		AuthToken:                   &password,
		AutomaticFailoverEnabled:    aws.Bool(i.AutomaticFailoverEnabled),
		ReplicationGroupId:          aws.String(i.ClusterID),
		CacheNodeType:               aws.String(i.CacheNodeType),
		CacheSubnetGroupName:        aws.String(i.DbSubnetGroup),
		SecurityGroupIds:            securityGroups,
		Engine:                      aws.String("redis"),
		NumCacheClusters:            numCacheClusters,
		Port:                        aws.Int32(6379),
		PreferredMaintenanceWindow:  aws.String(i.PreferredMaintenanceWindow),
		SnapshotWindow:              aws.String(i.SnapshotWindow),
		SnapshotRetentionLimit:      snapshotRetentionLimit,
		Tags:                        redisTags,
	}
	if i.EngineVersion != "" {
		params.EngineVersion = aws.String(i.EngineVersion)
	}
	return params, nil
}
