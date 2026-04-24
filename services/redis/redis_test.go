package redis

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
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
				t.Fatal("Did not receive expected RDS instance as create worker argument")
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
		})
	}
}

func TestAsyncDeleteRedis(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	notFoundErr := &elasticacheTypes.ReplicationGroupNotFoundFault{
		Message: aws.String("not found"),
	}

	testCases := map[string]struct {
		instance            *RedisInstance
		dbAdapter           *dedicatedRedisAdapter
		expectedState       base.InstanceState
		expectedRecordCount int64
	}{
		"success": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsResults: []*elasticache.DescribeSnapshotsOutput{
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
					},
				},
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
			expectedState: base.InstanceGone,
		},
		"error checking status": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{errors.New("error describing database instances")},
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error verifying deletion": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{errors.New("failed to delete")},
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error deleting": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					deleteReplicationGroupErr: errors.New("error deleting instance"),
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error describing initial snapshot": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsErrors:       []error{errors.New("describe snapshot error")},
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error copying snapshot": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsResults: []*elasticache.DescribeSnapshotsOutput{
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
					},
					copySnapshotErr: errors.New("copy snapshot error"),
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error writing snapshot to S3": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsResults: []*elasticache.DescribeSnapshotsOutput{
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
					},
				},
				&mockS3Client{
					putObjectErr: errors.New("error writing to s3"),
				},
			),
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error describing snapshot copy": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsResults: []*elasticache.DescribeSnapshotsOutput{
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
					},
					describeSnapshotsErrors: []error{nil, errors.New("error describing snapshot")},
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error deleting snapshot": {
			dbAdapter: NewTestDedicatedRedisAdapter(
				t.Context(),
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				brokerDB,
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsResults: []*elasticache.DescribeSnapshotsOutput{
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
						{
							Snapshots: []elasticacheTypes.Snapshot{
								{
									SnapshotStatus: aws.String("available"),
								},
							},
						},
					},
					deleteSnapshotErr: errors.New("error deleting snapshot"),
				},
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
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := brokerDB.Create(test.instance).Error
			if err != nil {
				t.Fatal(err)
			}

			var count int64
			brokerDB.Where("uuid = ?", test.instance.Uuid).First(test.instance).Count(&count)
			if count == 0 {
				t.Fatal("The instance should be in the DB")
			}

			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			test.dbAdapter.asyncDeleteRedis(test.instance)

			asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.instance.ServiceID, test.instance.Uuid, base.DeleteOp)
			if err != nil {
				t.Fatal(err)
			}

			if asyncJobMsg.JobState.State != test.expectedState {
				t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}

			brokerDB.Where("uuid = ?", test.instance.Uuid).First(test.instance).Count(&count)
			if count != test.expectedRecordCount {
				t.Fatalf("expected %d records, found %d", test.expectedRecordCount, count)
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
		instance               *RedisInstance
		adapter                *dedicatedRedisAdapter
		expectedErr            error
		expectedState          base.InstanceState
		password               string
		expectedAsyncJobStates []base.InstanceState
	}{
		"success": {
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
			expectedState:          base.InstanceInProgress,
			expectedAsyncJobStates: []base.InstanceState{base.InstanceInProgress, base.InstanceGone},
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

			if len(test.expectedAsyncJobStates) > 0 {
				asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.instance.ServiceID, test.instance.Uuid, base.DeleteOp)
				if err != nil {
					t.Fatal(err)
				}

				// The exact database state at this point in the test is non-deterministic, since the database updates
				// are being done in a goroutine. So we test against a set of possible job states
				if !slices.Contains(test.expectedAsyncJobStates, asyncJobMsg.JobState.State) {
					t.Fatalf("expected one of async job states: %+v, got: %s", test.expectedAsyncJobStates, asyncJobMsg.JobState.State)
				}
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
		})
	}
}
