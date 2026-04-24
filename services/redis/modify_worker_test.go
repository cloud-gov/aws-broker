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
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
)

func TestAsyncModifyRedis(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx              context.Context
		instance         *RedisInstance
		worker           *ModifyWorker
		expectedState    base.InstanceState
		expectedInstance *RedisInstance
		plan             *catalog.RDSPlan
	}{
		"success": {
			ctx: context.Background(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRedisClient{},
				slog.New(&testutil.MockLogHandler{}),
			),
			plan: &catalog.RDSPlan{},
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
			},
			expectedState: base.InstanceReady,
		},
		"error modifying redis isntance": {
			ctx: context.Background(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRedisClient{
					modifyReplicationGroupErr: errors.New("error modifying redis"),
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
			expectedState: base.InstanceNotModified,
		},
		"error increasing replica count": {
			ctx: context.Background(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRedisClient{
					increaseReplicaCountErr: errors.New("error increasing replica count"),
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
				NewReplicaCount: 1,
			},
			expectedState: base.InstanceNotModified,
		},
		"success with increased replica count": {
			ctx: context.Background(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("available"),
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{
													CurrentRole: aws.String("replica"),
												},
											},
										},
									},
								},
							},
						},
					},
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
				NewReplicaCount: 1,
			},
			expectedState: base.InstanceReady,
		},
		"failure waiting for increased replica count": {
			ctx: context.Background(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRedisClient{
					describeReplicationGroupsErrs: []error{
						errors.New("error waiting for replication group"),
					},
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
				NewReplicaCount: 1,
			},
			expectedState: base.InstanceNotModified,
		},
		"success waiting for increase replica count on retry": {
			ctx: context.Background(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("modifying"),
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("modifying"),
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("available"),
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{
													CurrentRole: aws.String("replica"),
												},
											},
										},
									},
								},
							},
						},
					},
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
				NewReplicaCount: 1,
			},
			expectedState: base.InstanceReady,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			test.worker.asyncModifyRedis(test.ctx, test.instance)

			asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.instance.ServiceID, test.instance.Uuid, base.ModifyOp)
			if err != nil {
				t.Fatal(err)
			}

			if test.expectedState != asyncJobMsg.JobState.State {
				t.Fatalf("expected async job state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}
		})
	}
}

func TestPrepareModifyReplicationGroupInput(t *testing.T) {
	testCases := map[string]struct {
		redisInstance  *RedisInstance
		password       string
		accessPolicy   string
		expectedParams *elasticache.ModifyReplicationGroupInput
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
				Engine:                     "redis",
			},
			expectedParams: &elasticache.ModifyReplicationGroupInput{
				ReplicationGroupDescription: aws.String("description"),
				AutomaticFailoverEnabled:    aws.Bool(true),
				ReplicationGroupId:          aws.String("cluster-1"),
				CacheNodeType:               aws.String("node-type"),
				SecurityGroupIds:            []string{"sec-group-1"},
				Engine:                      aws.String("redis"),
				PreferredMaintenanceWindow:  aws.String("1AM"),
				SnapshotWindow:              aws.String("4AM"),
				SnapshotRetentionLimit:      aws.Int32(int32(14)),
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
			expectedParams: &elasticache.ModifyReplicationGroupInput{
				ReplicationGroupDescription: aws.String("description"),
				AutomaticFailoverEnabled:    aws.Bool(true),
				ReplicationGroupId:          aws.String("cluster-1"),
				CacheNodeType:               aws.String("node-type"),
				SecurityGroupIds:            []string{"sec-group-1"},
				Engine:                      aws.String("redis"),
				PreferredMaintenanceWindow:  aws.String("1AM"),
				SnapshotWindow:              aws.String("4AM"),
				SnapshotRetentionLimit:      aws.Int32(int32(14)),
				EngineVersion:               aws.String("7.0"),
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := prepareModifyReplicationGroupInput(
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
