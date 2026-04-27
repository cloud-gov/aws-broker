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
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
)

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

			err = test.worker.asyncDeleteRedis(test.ctx, test.instance)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error but received none")
			}

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
