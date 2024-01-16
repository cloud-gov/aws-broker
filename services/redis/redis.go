package redis

import (
	"encoding/json"
	"errors"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jinzhu/gorm"

	"bytes"
	"fmt"
	"log"
)

type redisAdapter interface {
	createRedis(i *RedisInstance, password string) (base.InstanceState, error)
	modifyRedis(i *RedisInstance, password string) (base.InstanceState, error)
	checkRedisStatus(i *RedisInstance) (base.InstanceState, error)
	bindRedisToApp(i *RedisInstance, password string) (map[string]string, error)
	deleteRedis(i *RedisInstance) (base.InstanceState, error)
}

type mockRedisAdapter struct {
}

func (d *mockRedisAdapter) createRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockRedisAdapter) modifyRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockRedisAdapter) checkRedisStatus(i *RedisInstance) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockRedisAdapter) bindRedisToApp(i *RedisInstance, password string) (map[string]string, error) {
	// TODO
	return i.getCredentials(password)
}

func (d *mockRedisAdapter) deleteRedis(i *RedisInstance) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}

type sharedRedisAdapter struct {
	SharedRedisConn *gorm.DB
}

func (d *sharedRedisAdapter) createRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *sharedRedisAdapter) modifyRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *sharedRedisAdapter) checkRedisStatus(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *sharedRedisAdapter) bindDBToApp(i *RedisInstance, password string) (map[string]string, error) {
	return i.getCredentials(password)
}

func (d *sharedRedisAdapter) deleteRedis(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceGone, nil
}

type dedicatedRedisAdapter struct {
	Plan        catalog.RedisPlan
	settings    config.Settings
	logger      lager.Logger
	elasticache elasticacheiface.ElastiCacheAPI
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-redis-broker-"

func (d *dedicatedRedisAdapter) createRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	// Standard parameters
	params := prepareCreateReplicationGroupInput(i, password)

	resp, err := d.elasticache.CreateReplicationGroup(params)

	// Pretty-print the response data.
	log.Println(awsutil.StringValue(resp))
	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
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

		resp, err := d.elasticache.DescribeReplicationGroups(params)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				// Generic AWS error with Code, Message, and original error (if any)
				fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// A service error occurred
					fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				}
			} else {
				// This case should never be hit, the SDK should always return an
				// error which satisfies the awserr.Error interface.
				fmt.Println(err.Error())
			}
			return base.InstanceNotCreated, err
		}

		numOfInstances := len(resp.ReplicationGroups)
		if numOfInstances > 0 {
			for _, value := range resp.ReplicationGroups {
				fmt.Println("Redis Instance:" + i.ClusterID + " is " + *(value.Status))
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
			return base.InstanceNotCreated, errors.New("Couldn't find any instances.")
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

		resp, err := d.elasticache.DescribeReplicationGroups(params)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				// Generic AWS error with Code, Message, and original error (if any)
				fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// A service error occurred
					fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				}
			} else {
				// This case should never be hit, the SDK should always return an
				// error which satisfies the awserr.Error interface.
				fmt.Println(err.Error())
			}
			return nil, err
		}

		numOfInstances := len(resp.ReplicationGroups)
		if numOfInstances > 0 {
			for _, value := range resp.ReplicationGroups {
				// First check that the instance is up.
				if value.Status != nil && *(value.Status) == "available" {
					if value.NodeGroups[0].PrimaryEndpoint != nil && value.NodeGroups[0].PrimaryEndpoint.Address != nil && value.NodeGroups[0].PrimaryEndpoint.Port != nil {
						fmt.Printf("host: %s port: %d \n", *(value.NodeGroups[0].PrimaryEndpoint.Address), *(value.NodeGroups[0].PrimaryEndpoint.Port))
						i.Port = *(value.NodeGroups[0].PrimaryEndpoint.Port)
						i.Host = *(value.NodeGroups[0].PrimaryEndpoint.Address)
						i.State = base.InstanceReady
						// Should only be one regardless. Just return now.
						break
					} else {
						// Something went horribly wrong. Should never get here.
						return nil, errors.New("Invalid memory for endpoint and/or endpoint members.")
					}
				} else {
					// Instance not up yet.
					return nil, errors.New("Instance not available yet. Please wait and try again..")
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
	_, err := d.elasticache.DeleteReplicationGroup(params)

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		go d.exportRedisSnapshot(i)
		return base.InstanceGone, nil
	}
	return base.InstanceNotGone, nil
}

func (d *dedicatedRedisAdapter) didAwsCallSucceed(err error) bool {
	// TODO Eventually return a formatted error object.
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
		return false
	}
	return true
}

func (d *dedicatedRedisAdapter) exportRedisSnapshot(i *RedisInstance) {
	aws_session, err := session.NewSession(aws.NewConfig().WithRegion(d.settings.Region))
	if success := d.didAwsCallSucceed(err); !success {
		d.logger.Error("exportRedisSnapshot: aws.NewSession Failed", err)
		return
	}
	path := i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
	bucket := d.settings.SnapshotsBucketName
	s3_svc := s3.New(aws_session)
	snapshot_name := i.ClusterID + "-final"
	sleep := 30 * time.Second
	d.logger.Info("exportRedisSnapshot: Waiting for Instance Snapshot to Complete", lager.Data{"uuid": i.Uuid})
	// poll for snapshot being available
	check_input := &elasticache.DescribeSnapshotsInput{
		SnapshotName: &snapshot_name,
	}
	for {
		resp, err := d.elasticache.DescribeSnapshots(check_input)
		if success := d.didAwsCallSucceed(err); !success {
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
	_, err = d.elasticache.CopySnapshot(copy_input)
	if success := d.didAwsCallSucceed(err); !success {
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
	input := s3.PutObjectInput{
		Body:                 body,
		Bucket:               aws.String(bucket),
		Key:                  aws.String(path + "/instance_manifest.json"),
		ServerSideEncryption: aws.String("AES256"),
	}
	// drop info to s3
	_, err = s3_svc.PutObject(&input)
	// Decide if AWS service call was successful
	if success := d.didAwsCallSucceed(err); !success {
		d.logger.Error("exportRedisSnapshot: S3.PutObject Failed", err, lager.Data{"uuid": i.Uuid})
		return
	}

	d.logger.Info("exportRedisSnapshot: Waiting for Instance Snapshot Copy to Complete", lager.Data{"uuid": i.Uuid})
	// poll for snapshot being available again before delete
	check_input = &elasticache.DescribeSnapshotsInput{
		SnapshotName: &snapshot_name,
	}
	for {
		resp, err := d.elasticache.DescribeSnapshots(check_input)
		if success := d.didAwsCallSucceed(err); !success {
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
	_, err = d.elasticache.DeleteSnapshot(delete_input)
	if success := d.didAwsCallSucceed(err); !success {
		d.logger.Error("Redis.DeleteSnapshot: Failed", err, lager.Data{"uuid": i.Uuid})
		return
	}
	d.logger.Info("exportRedisSnapshot: Snapshot and Manifest backup to s3 Complete.", lager.Data{"uuid": i.Uuid})
}

func prepareCreateReplicationGroupInput(
	i *RedisInstance,
	password string,
) *elasticache.CreateReplicationGroupInput {
	var redisTags []*elasticache.Tag
	for k, v := range i.Tags {
		tag := elasticache.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		redisTags = append(redisTags, &tag)
	}

	var securityGroups []*string

	securityGroups = append(securityGroups, &i.SecGroup)

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
		NumCacheClusters:            aws.Int64(int64(i.NumCacheClusters)),
		Port:                        aws.Int64(6379),
		PreferredMaintenanceWindow:  aws.String(i.PreferredMaintenanceWindow),
		SnapshotWindow:              aws.String(i.SnapshotWindow),
		SnapshotRetentionLimit:      aws.Int64(int64(i.SnapshotRetentionLimit)),
		Tags:                        redisTags,
	}
	if i.EngineVersion != "" {
		params.EngineVersion = aws.String(i.EngineVersion)
	}
	return params
}
