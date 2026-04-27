package redis

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"

	"fmt"
)

type redisAdapter interface {
	createRedis(i *RedisInstance) (base.InstanceState, error)
	modifyRedis(i *RedisInstance) (base.InstanceState, error)
	checkRedisStatus(i *RedisInstance) (base.InstanceState, error)
	bindRedisToApp(i *RedisInstance, password string) (map[string]string, error)
	deleteRedis(i *RedisInstance) (base.InstanceState, error)
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(
	ctx context.Context,
	s *config.Settings,
	db *gorm.DB,
	logger *slog.Logger,
	riverClient *river.Client[*sql.Tx],
) (redisAdapter, error) {
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

	redisAdapter = NewRedisDedicatedDBAdapter(ctx, s, db, elasticacheClient, logger, riverClient)
	return redisAdapter, nil
}

func NewRedisDedicatedDBAdapter(
	ctx context.Context,
	s *config.Settings,
	db *gorm.DB,
	elasticache ElasticacheClientInterface,
	logger *slog.Logger,
	riverClient *river.Client[*sql.Tx],
) *dedicatedRedisAdapter {
	return &dedicatedRedisAdapter{
		ctx:         ctx,
		settings:    *s,
		db:          db,
		logger:      logger,
		elasticache: elasticache,
		riverClient: riverClient,
	}
}

type mockRedisAdapter struct {
}

func (d *mockRedisAdapter) createRedis(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

func (d *mockRedisAdapter) modifyRedis(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

func (d *mockRedisAdapter) checkRedisStatus(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *mockRedisAdapter) bindRedisToApp(i *RedisInstance, password string) (map[string]string, error) {
	return i.getCredentials(password)
}

func (d *mockRedisAdapter) deleteRedis(i *RedisInstance) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

type dedicatedRedisAdapter struct {
	ctx         context.Context
	settings    config.Settings
	logger      *slog.Logger
	elasticache ElasticacheClientInterface
	db          *gorm.DB
	riverClient *river.Client[*sql.Tx]
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-redis-broker-"

func (d *dedicatedRedisAdapter) createRedis(i *RedisInstance) (base.InstanceState, error) {
	// Standard parameters
	params, err := prepareCreateReplicationGroupInput(i)
	if err != nil {
		d.logger.Error("prepareCreateReplicationGroupInput", "err", err)
		return base.InstanceNotCreated, err
	}

	_, err = d.elasticache.CreateReplicationGroup(context.TODO(), params)
	if err != nil {
		d.logger.Error("CreateReplicationGroup", "err", err)
		return base.InstanceNotCreated, err
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedRedisAdapter) modifyRedis(i *RedisInstance) (base.InstanceState, error) {
	err := asyncmessage.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.ModifyOp, base.InstanceInProgress, "Modification in progress")
	if err != nil {
		return base.InstanceNotModified, err
	}

	tx := d.db.Begin()
	if err := tx.Error; err != nil {
		return base.InstanceNotModified, err
	}
	defer tx.Rollback()

	sqlTx := tx.Statement.ConnPool.(*sql.Tx)

	_, err = d.riverClient.InsertTx(d.ctx, sqlTx, &ModifyArgs{
		Instance: i,
	}, nil)
	if err != nil {
		return base.InstanceNotModified, err
	}

	if err := tx.Commit().Error; err != nil {
		return base.InstanceNotModified, err
	}

	return base.InstanceInProgress, nil
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
			d.logger.Error("checkRedisStatus: DescribeReplicationGroups failed", "err", err)
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
			d.logger.Error("bindRedisToApp: DescribeReplicationGroups failed", "err", err)
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
	err := asyncmessage.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.DeleteOp, base.InstanceInProgress, "Deletion in progress")
	if err != nil {
		return base.InstanceNotGone, err
	}

	tx := d.db.Begin()
	if err := tx.Error; err != nil {
		return base.InstanceNotGone, err
	}
	defer tx.Rollback()

	sqlTx := tx.Statement.ConnPool.(*sql.Tx)

	_, err = d.riverClient.InsertTx(d.ctx, sqlTx, &DeleteArgs{
		Instance: i,
	}, nil)
	if err != nil {
		return base.InstanceNotGone, err
	}

	if err := tx.Commit().Error; err != nil {
		return base.InstanceNotGone, err
	}

	return base.InstanceInProgress, nil
}

func prepareCreateReplicationGroupInput(i *RedisInstance) (*elasticache.CreateReplicationGroupInput, error) {
	redisTags := ConvertTagsToElasticacheTags(i.Tags)

	securityGroups := []string{i.SecGroup}

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
		AuthToken:                   &i.ClearPassword,
		AutomaticFailoverEnabled:    aws.Bool(i.AutomaticFailoverEnabled),
		ReplicationGroupId:          aws.String(i.ClusterID),
		CacheNodeType:               aws.String(i.CacheNodeType),
		CacheSubnetGroupName:        aws.String(i.DbSubnetGroup),
		SecurityGroupIds:            securityGroups,
		Engine:                      aws.String(i.Engine),
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
