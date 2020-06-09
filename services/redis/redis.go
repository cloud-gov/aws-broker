package redis

import (
	"errors"

	"github.com/18F/aws-broker/base"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"

	"fmt"
	"log"
)

type redisAdapter interface {
	createRedis(i *RedisInstance, password string) (base.InstanceState, error)
	checkRedisStatus(i *RedisInstance) (base.InstanceState, error)
	bindRedisToApp(i *RedisInstance, password string) (map[string]string, error)
	deleteRedis(i *RedisInstance) (base.InstanceState, error)
}

type sharedRedisAdapter struct {
	SharedRedisConn *gorm.DB
}

func (d *sharedRedisAdapter) createDB(i *RedisInstance, password string) (base.InstanceState, error) {
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
	Plan     catalog.RedisPlan
	settings config.Settings
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-redis-broker-"

func (d *dedicatedRedisAdapter) createRedis(i *RedisInstance, password string) (base.InstanceState, error) {
	svc := elasticache.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	// Elasticache Redis does not currently support custom tags (possibly in the future)
	var redisTags []*elasticache.Tag

	for k, v := range i.Tags {
		var tag elasticache.Tag
		tag = elasticache.Tag{
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
		EngineVersion:               aws.String(i.EngineVersion),
		NumCacheClusters:            aws.Int64(int64(i.NumCacheClusters)),
		Port:                        aws.Int64(6379),
		CacheParameterGroupName:     aws.String(i.ParameterGroup),
		PreferredMaintenanceWindow:  aws.String(i.PreferredMaintenanceWindow),
		SnapshotWindow:              aws.String(i.SnapshotWindow),
		SnapshotRetentionLimit:      aws.Int64(int64(i.SnapshotRetentionLimit)),
	}

	resp, err := svc.CreateReplicationGroup(params)
	// Pretty-print the response data.
	log.Println(awsutil.StringValue(resp))
	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

func (d *dedicatedRedisAdapter) checkRedisStatus(i *RedisInstance) (base.InstanceState, error) {
	// First, we need to check if the instance state
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		svc := elasticache.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String(i.ClusterID), // Required
		}

		resp, err := svc.DescribeReplicationGroups(params)
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

		// Pretty-print the response data.
		fmt.Println(awsutil.StringValue(resp))

		numOfInstances := len(resp.ReplicationGroups)
		if numOfInstances > 0 {
			for _, value := range resp.ReplicationGroups {
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
		svc := elasticache.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String(i.ClusterID), // Required
		}

		resp, err := svc.DescribeReplicationGroups(params)
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

		// Pretty-print the response data.
		fmt.Println(awsutil.StringValue(resp))

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
	svc := elasticache.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	params := &elasticache.DeleteReplicationGroupInput{
		ReplicationGroupId: aws.String(i.ClusterID), // Required
	}
	resp, err := svc.DeleteReplicationGroup(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
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
