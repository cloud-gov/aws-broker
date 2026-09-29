package redis

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/riverqueue/river"
)

func TestDeleteWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		instance      *RedisInstance
		expectedState base.InstanceState
		password      string
		expectErr     bool
		worker        *DeleteWorker
	}{
		"success": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{&elasticacheTypes.ReplicationGroupNotFoundFault{
						Message: aws.String("not found"),
					}},
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
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedState: base.InstanceGone,
		},
		"failure": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRedisClient{
					deleteReplicationGroupErr: errors.New("failure"),
				},
				&mockS3Client{},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotGone,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.worker.Work(test.ctx, &river.Job[DeleteArgs]{Args: DeleteArgs{
				Instance: test.instance,
			}})
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
			asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.instance.ServiceID, test.instance.Uuid, base.DeleteOp)
			if err != nil {
				t.Fatal(err)
			}

			if test.expectedState != asyncJobMsg.JobState.State {
				t.Fatalf("expected async job state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
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
		ctx                 context.Context
		instance            *RedisInstance
		worker              *DeleteWorker
		expectedState       base.InstanceState
		expectedRecordCount int64
		expectErr           bool
	}{
		"success": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},

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
				slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{errors.New("error describing database instances")},
				},
				&mockS3Client{},
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error verifying deletion": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},

				&mockRedisClient{
					describeReplicationGroupsErrs: []error{errors.New("failed to delete")},
				},
				&mockS3Client{},
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error deleting": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				&mockRedisClient{
					deleteReplicationGroupErr: errors.New("error deleting instance"),
				},
				&mockS3Client{},
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error describing initial snapshot": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{notFoundErr},
					describeSnapshotsErrors:       []error{errors.New("describe snapshot error")},
				},
				&mockS3Client{},
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error copying snapshot": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
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
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error writing snapshot to S3": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
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
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error describing snapshot copy": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
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
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"error deleting snapshot": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
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
				slog.New(&testutil.MockLogHandler{}),
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
			expectErr:           true,
		},
		"replication group already gone": {
			ctx: t.Context(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},

				&mockRedisClient{
					deleteReplicationGroupErr: notFoundErr,
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
				slog.New(&testutil.MockLogHandler{}),
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

			err = test.worker.asyncDeleteRedis(test.ctx, test.instance, base.DeleteOp)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error but received none")
			}

			brokerDB.Where("uuid = ?", test.instance.Uuid).First(test.instance).Count(&count)
			if count != test.expectedRecordCount {
				t.Fatalf("expected %d records, found %d", test.expectedRecordCount, count)
			}
		})
	}
}
