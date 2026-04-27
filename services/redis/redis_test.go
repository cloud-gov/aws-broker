package redis

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/go-test/deep"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivertest"
)

func TestPrepareCreateReplicationGroupInput(t *testing.T) {
	testCases := map[string]struct {
		redisInstance  *RedisInstance
		password       string
		accessPolicy   string
		expectedParams *elasticache.CreateReplicationGroupInput
	}{
		"sets properties correctly": {
			redisInstance: &RedisInstance{
				Description:              "description",
				AutomaticFailoverEnabled: true,
				Tags: map[string]string{
					"foo": "bar",
				},
				ClusterID:                  "cluster-1",
				CacheNodeType:              "node-type",
				DbSubnetGroup:              "db-group-1",
				SecGroup:                   "sec-group-1",
				NumCacheClusters:           3,
				PreferredMaintenanceWindow: "1AM",
				SnapshotWindow:             "4AM",
				SnapshotRetentionLimit:     14,
				ClearPassword:              "fake-password",
				Engine:                     "valkey",
			},

			expectedParams: &elasticache.CreateReplicationGroupInput{
				AtRestEncryptionEnabled:     aws.Bool(true),
				TransitEncryptionEnabled:    aws.Bool(true),
				AutoMinorVersionUpgrade:     aws.Bool(true),
				ReplicationGroupDescription: aws.String("description"),
				AuthToken:                   aws.String("fake-password"),
				AutomaticFailoverEnabled:    aws.Bool(true),
				ReplicationGroupId:          aws.String("cluster-1"),
				CacheNodeType:               aws.String("node-type"),
				CacheSubnetGroupName:        aws.String("db-group-1"),
				SecurityGroupIds:            []string{"sec-group-1"},
				Engine:                      aws.String("valkey"),
				NumCacheClusters:            aws.Int32(int32(3)),
				Port:                        aws.Int32(6379),
				PreferredMaintenanceWindow:  aws.String("1AM"),
				SnapshotWindow:              aws.String("4AM"),
				SnapshotRetentionLimit:      aws.Int32(int32(14)),
				Tags: []elasticacheTypes.Tag{
					{
						Key:   aws.String("foo"),
						Value: aws.String("bar"),
					},
				},
			},
		},
		"sets engine version": {
			redisInstance: &RedisInstance{
				Description:              "description",
				AutomaticFailoverEnabled: true,
				Tags: map[string]string{
					"foo": "bar",
				},
				ClusterID:                  "cluster-1",
				CacheNodeType:              "node-type",
				DbSubnetGroup:              "db-group-1",
				SecGroup:                   "sec-group-1",
				NumCacheClusters:           3,
				PreferredMaintenanceWindow: "1AM",
				SnapshotWindow:             "4AM",
				SnapshotRetentionLimit:     14,
				EngineVersion:              "7.0",
				ClearPassword:              "fake-password",
				Engine:                     "redis",
			},
			expectedParams: &elasticache.CreateReplicationGroupInput{
				AtRestEncryptionEnabled:     aws.Bool(true),
				TransitEncryptionEnabled:    aws.Bool(true),
				AutoMinorVersionUpgrade:     aws.Bool(true),
				ReplicationGroupDescription: aws.String("description"),
				AuthToken:                   aws.String("fake-password"),
				AutomaticFailoverEnabled:    aws.Bool(true),
				ReplicationGroupId:          aws.String("cluster-1"),
				CacheNodeType:               aws.String("node-type"),
				CacheSubnetGroupName:        aws.String("db-group-1"),
				SecurityGroupIds:            []string{"sec-group-1"},
				Engine:                      aws.String("redis"),
				NumCacheClusters:            aws.Int32(int32(3)),
				Port:                        aws.Int32(6379),
				PreferredMaintenanceWindow:  aws.String("1AM"),
				SnapshotWindow:              aws.String("4AM"),
				SnapshotRetentionLimit:      aws.Int32(int32(14)),
				Tags: []elasticacheTypes.Tag{
					{
						Key:   aws.String("foo"),
						Value: aws.String("bar"),
					},
				},
				EngineVersion: aws.String("7.0"),
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := prepareCreateReplicationGroupInput(
				test.redisInstance,
			)
			if err != nil {
				t.Fatal(err)
			}
			if diff := deep.Equal(params, test.expectedParams); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestModifyRedis(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx                    context.Context
		instance               *RedisInstance
		adapter                redisAdapter
		expectedErr            error
		expectedState          base.InstanceState
		expectedAsyncJobStates []base.InstanceState
	}{
		"success": {
			ctx: t.Context(),
			adapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{},
				brokerDB,
				&mockRedisClient{},
				&mockS3Client{},
			),
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			expectedState: base.InstanceInProgress,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.adapter.modifyRedis(test.instance)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			tx := brokerDB.Begin()
			if err := tx.Error; err != nil {
				t.Fatal(err)
			}

			sqlTx := tx.Statement.ConnPool.(*sql.Tx)
			defer tx.Rollback()

			job := rivertest.RequireInsertedTx[*riversqlite.Driver](test.ctx, t, sqlTx, &ModifyArgs{}, nil)

			if job.Args.Instance.Uuid != test.instance.Uuid {
				t.Fatal("Did not receive expected RDS instance as modify worker argument")
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
		})
	}
}

func TestDeleteRedis(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	notFoundErr := &elasticacheTypes.ReplicationGroupNotFoundFault{
		Message: aws.String("not found"),
	}

	testCases := map[string]struct {
		ctx           context.Context
		instance      *RedisInstance
		adapter       *dedicatedRedisAdapter
		expectedErr   error
		expectedState base.InstanceState
		password      string
	}{
		"success": {
			ctx: t.Context(),
			adapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay: 1 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
				},
				&mockS3Client{},
			),
			password: helpers.RandStr(10),
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			expectedState: base.InstanceInProgress,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.adapter.deleteRedis(test.instance)

			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}

			tx := brokerDB.Begin()
			if err := tx.Error; err != nil {
				t.Fatal(err)
			}

			sqlTx := tx.Statement.ConnPool.(*sql.Tx)
			defer tx.Rollback()

			job := rivertest.RequireInsertedTx[*riversqlite.Driver](test.ctx, t, sqlTx, &DeleteArgs{}, nil)

			if job.Args.Instance.Uuid != test.instance.Uuid {
				t.Fatal("Did not receive expected instance as delete worker argument")
			}
		})
	}
}
