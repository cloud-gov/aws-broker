package redis

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
	"github.com/riverqueue/river"
)

func TestModifyWorkerWork(t *testing.T) {
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
		worker        *ModifyWorker
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
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Second,
					PollAwsMinDelay:    1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
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
									Status: aws.String("available"),
								},
							},
						},
					},
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedState: base.InstanceReady,
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
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRedisClient{
					modifyReplicationGroupErr: errors.New("failure"),
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotModified,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.worker.Work(test.ctx, &river.Job[ModifyArgs]{Args: ModifyArgs{
				Instance: test.instance,
			}})
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
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

func TestAsyncModifyRedis(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx              context.Context
		instance         *RedisInstance
		worker           *ModifyWorker
		expectedInstance *RedisInstance
		plan             *catalog.RDSPlan
		expectErr        bool
	}{
		"success": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
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
									Status: aws.String("available"),
									Engine: aws.String("valkey"),
								},
							},
						},
					},
				},
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
				Engine: "valkey",
			},
		},
		"error modifying redis isntance": {
			ctx: t.Context(),
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
			expectErr: true,
		},
		"error increasing replica count": {
			ctx: t.Context(),
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
			expectErr: true,
		},
		"success with increased replica count": {
			ctx: t.Context(),
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
									Status: aws.String("available"),
									Engine: aws.String("valkey"),
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
				Engine:          "valkey",
			},
		},
		"failure waiting for increased replica count": {
			ctx: t.Context(),
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
			expectErr: true,
		},
		"success waiting for increase replica count on retry": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
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
									Status: aws.String("available"),
									Engine: aws.String("valkey"),
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
				Engine:          "valkey",
			},
		},
		"replica never appears before attempts are exhausted": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
					PollAwsMaxRetries:  2,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						// Consumed by the SDK waiter.
						availableWithoutReplica(),
						// Consumed by the verify loop: 1 initial attempt + 2 retries.
						availableWithoutReplica(),
						availableWithoutReplica(),
						availableWithoutReplica(),
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
			expectErr: true,
		},
		"waits for replication group to become available after modify": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
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
									Status: aws.String("available"),
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("available"),
									Engine: aws.String("valkey"),
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
				Engine: "valkey",
			},
		},
		"fails if replication group never becomes available after modify": {
			ctx: t.Context(),
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
									Status: aws.String("modifying"),
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
				Engine: "valkey",
			},
			expectErr: true,
		},
		"fails if engine does not match after replication group becomes available": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
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
									Status: aws.String("available"),
									Engine: aws.String("redis"),
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
				Engine: "valkey",
			},
			expectErr: true,
		},
		"increases shard count before modifying replication group": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						// 1: currentShardCount check before resharding (3 != target of 5)
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{{}, {}, {}},
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{Status: aws.String("available")},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{{}, {}, {}, {}, {}},
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{Status: aws.String("available")},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("available"),
									Engine: aws.String("valkey"),
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
				Engine:           "valkey",
				NumNodeGroups:    5,
				NewNumNodeGroups: 5,
			},
		},
		"error increasing shard count": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						// currentShardCount check before resharding (0 != target of 5)
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{{}},
						},
					},
					modifyReplicationGroupShardConfigErr: errors.New("error increasing shard count"),
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
				NewNumNodeGroups: 5,
			},
			expectErr: true,
		},
		"cluster mode enabled increases replicas per shard via ReplicaConfiguration": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{NodeGroupId: aws.String("0001")},
										{NodeGroupId: aws.String("0002")},
										{NodeGroupId: aws.String("0003")},
									},
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{Status: aws.String("available")},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
												{CurrentRole: aws.String("replica")},
											},
										},
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
												{CurrentRole: aws.String("replica")},
											},
										},
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
												{CurrentRole: aws.String("replica")},
											},
										},
									},
								},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{Status: aws.String("available")},
							},
						},
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									Status: aws.String("available"),
									Engine: aws.String("valkey"),
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
				Engine:               "valkey",
				NumNodeGroups:        3,
				ReplicasPerNodeGroup: 2,
				NewReplicaCount:      2,
			},
		},
		"migrates cluster-mode-disabled instance to cluster-mode-enabled": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						// 1: setClusterMode(Compatible) checking the current mode (unset, so the transition proceeds)
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{}}},
						// 2: waiter after setting ClusterMode=Compatible
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 3: increaseShardCount checking the current shard count (1, cluster-mode-disabled, so the reshard proceeds)
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{NodeGroups: []elasticacheTypes.NodeGroup{{}}}}},
						// 4: waiter after ModifyReplicationGroupShardConfiguration
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 5: verifyIncreasedShardCount
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{{}, {}, {}},
								},
							},
						},
						// 6: reconcileReplicaCountPerShard reading current per-shard replica counts
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
									},
								},
							},
						},
						// 7: verifyIncreasedReplicaCount (no increase/decrease needed; both sides already match)
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
									},
								},
							},
						},
						// 8: setClusterMode(Enabled) checking the current mode (still compatible, so the transition proceeds)
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{ClusterMode: elasticacheTypes.ClusterModeCompatible}}},
						// 9: waiter after setting ClusterMode=Enabled
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 10: waiter after the ordinary ModifyReplicationGroup call
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 11: verifyModifiedEngineAndVersion
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available"), Engine: aws.String("valkey")}}},
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
				ClusterID:              "cluster-1",
				Engine:                 "valkey",
				EngineVersion:          "8.2",
				MigratingToClusterMode: true,
				NumNodeGroups:          3,
				NewNumNodeGroups:       3,
			},
		},
		"migration is resumable: setClusterMode no-ops when the replication group already reports the target mode": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						// 1: setClusterMode(Compatible) checking the current mode: a previous
						// attempt already reached Compatible, so this step must skip straight
						// to waiting for availability rather than resending the transition.
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{ClusterMode: elasticacheTypes.ClusterModeCompatible}}},
						// 2: waiter consumed by that skip-to-available path
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 3: increaseShardCount checking the current shard count: a previous
						// attempt already resharded to 3, so this step must skip the AWS call.
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{{}, {}, {}},
								},
							},
						},
						// 4: reconcileReplicaCountPerShard reading current per-shard replica counts
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
									},
								},
							},
						},
						// 5: verifyIncreasedReplicaCount (no increase/decrease needed; both sides already match)
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
										{
											Status:           aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{{CurrentRole: aws.String("primary")}},
										},
									},
								},
							},
						},
						// 6: setClusterMode(Enabled) checking the current mode (still compatible, so the transition proceeds)
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{ClusterMode: elasticacheTypes.ClusterModeCompatible}}},
						// 7: waiter after setting ClusterMode=Enabled
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 8: waiter after the ordinary ModifyReplicationGroup call
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 9: verifyModifiedEngineAndVersion
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available"), Engine: aws.String("valkey")}}},
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
				ClusterID:              "cluster-1",
				Engine:                 "valkey",
				EngineVersion:          "8.2",
				MigratingToClusterMode: true,
				NumNodeGroups:          3,
				NewNumNodeGroups:       3,
			},
		},
		"cluster-mode-enabled plan change decreases replicas per shard via DecreaseReplicaCount": {
			ctx: t.Context(),
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Second,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: []*elasticache.DescribeReplicationGroupsOutput{
						// 1: reconcileReplicaCountPerShard reading current per-shard replica
						// counts: every shard currently has 2 replicas, target is 1.
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											NodeGroupId: aws.String("0001"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
												{CurrentRole: aws.String("replica")},
											},
										},
										{
											NodeGroupId: aws.String("0002"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
												{CurrentRole: aws.String("replica")},
											},
										},
									},
								},
							},
						},
						// 2: waiter after DecreaseReplicaCount
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 3: verifyIncreasedReplicaCount
						{
							ReplicationGroups: []elasticacheTypes.ReplicationGroup{
								{
									NodeGroups: []elasticacheTypes.NodeGroup{
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
											},
										},
										{
											Status: aws.String("available"),
											NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
												{CurrentRole: aws.String("primary")},
												{CurrentRole: aws.String("replica")},
											},
										},
									},
								},
							},
						},
						// 4: waiter after the ordinary ModifyReplicationGroup call
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
						// 5: verifyModifiedEngineAndVersion
						{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available"), Engine: aws.String("valkey")}}},
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
				Engine:               "valkey",
				NumNodeGroups:        2,
				ReplicasPerNodeGroup: 1,
				NewReplicaCount:      1,
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.worker.asyncModifyRedis(test.ctx, test.instance, base.ModifyOp)
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
		})
	}
}

func availableWithoutReplica() *elasticache.DescribeReplicationGroupsOutput {
	return &elasticache.DescribeReplicationGroupsOutput{
		ReplicationGroups: []elasticacheTypes.ReplicationGroup{
			{
				Status: aws.String("available"),
				NodeGroups: []elasticacheTypes.NodeGroup{
					{
						Status: aws.String("available"),
						NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
							{CurrentRole: aws.String("primary")},
						},
					},
				},
			},
		},
	}
}

func TestVerifyIncreasedReplicaCount(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	availableWithReplica := func() *elasticache.DescribeReplicationGroupsOutput {
		return &elasticache.DescribeReplicationGroupsOutput{
			ReplicationGroups: []elasticacheTypes.ReplicationGroup{
				{
					NodeGroups: []elasticacheTypes.NodeGroup{
						{
							Status: aws.String("available"),
							NodeGroupMembers: []elasticacheTypes.NodeGroupMember{
								{CurrentRole: aws.String("primary")},
								{CurrentRole: aws.String("replica")},
							},
						},
					},
				},
			},
		}
	}

	testCases := map[string]struct {
		results         []*elasticache.DescribeReplicationGroupsOutput
		errs            []error
		newReplicaCount int
		maxRetries      int64
		expectErr       bool
		expectErrMsg    string
	}{
		"replica present on first attempt": {
			results:         []*elasticache.DescribeReplicationGroupsOutput{availableWithReplica()},
			newReplicaCount: 1,
			maxRetries:      2,
		},
		"replica appears on a later attempt": {
			results: []*elasticache.DescribeReplicationGroupsOutput{
				availableWithoutReplica(),
				availableWithReplica(),
			},
			newReplicaCount: 1,
			maxRetries:      2,
		},
		"replica never appears": {
			results: []*elasticache.DescribeReplicationGroupsOutput{
				availableWithoutReplica(),
				availableWithoutReplica(),
				availableWithoutReplica(),
			},
			newReplicaCount: 1,
			maxRetries:      2,
			expectErr:       true,
			expectErrMsg:    "did not report 1 replica nodes in every shard after 3 attempts",
		},
		"describe error is surfaced": {
			errs:            []error{errors.New("boom")},
			newReplicaCount: 1,
			maxRetries:      2,
			expectErr:       true,
			expectErrMsg:    "boom",
		},
		"empty response is surfaced rather than panicking": {
			results:         []*elasticache.DescribeReplicationGroupsOutput{{}},
			newReplicaCount: 1,
			maxRetries:      0,
			expectErr:       true,
			expectErrMsg:    "no replication groups",
		},
		"replication group with no node groups is surfaced": {
			results: []*elasticache.DescribeReplicationGroupsOutput{
				{ReplicationGroups: []elasticacheTypes.ReplicationGroup{{Status: aws.String("available")}}},
			},
			newReplicaCount: 1,
			maxRetries:      0,
			expectErr:       true,
			expectErrMsg:    "no node groups",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			worker := NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: test.maxRetries,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: test.results,
					describeReplicationGroupsErrs:    test.errs,
				},
				slog.New(&testutil.MockLogHandler{}),
			)

			instance := &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{ServiceID: helpers.RandStr(10)},
					Uuid:    helpers.RandStr(10),
				},
				ClusterID:       "cluster-1",
				NewReplicaCount: test.newReplicaCount,
			}

			err := worker.verifyIncreasedReplicaCount(t.Context(), instance)
			if test.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil: the caller treats nil as a completed resize")
				}
				if !strings.Contains(err.Error(), test.expectErrMsg) {
					t.Fatalf("expected error containing %q, got %q", test.expectErrMsg, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
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
				ApplyImmediately:            aws.Bool(true),
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
				ApplyImmediately:            aws.Bool(true),
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

func TestVerifyIncreasedShardCount(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	withNodeGroups := func(count int) *elasticache.DescribeReplicationGroupsOutput {
		nodeGroups := make([]elasticacheTypes.NodeGroup, count)
		return &elasticache.DescribeReplicationGroupsOutput{
			ReplicationGroups: []elasticacheTypes.ReplicationGroup{
				{NodeGroups: nodeGroups},
			},
		}
	}

	testCases := map[string]struct {
		results          []*elasticache.DescribeReplicationGroupsOutput
		errs             []error
		newNumNodeGroups int
		maxRetries       int64
		expectErr        bool
		expectErrMsg     string
	}{
		"shard count matches on first attempt": {
			results:          []*elasticache.DescribeReplicationGroupsOutput{withNodeGroups(5)},
			newNumNodeGroups: 5,
			maxRetries:       2,
		},
		"shard count matches on a later attempt": {
			results: []*elasticache.DescribeReplicationGroupsOutput{
				withNodeGroups(3),
				withNodeGroups(5),
			},
			newNumNodeGroups: 5,
			maxRetries:       2,
		},
		"shard count never matches": {
			results: []*elasticache.DescribeReplicationGroupsOutput{
				withNodeGroups(3),
				withNodeGroups(3),
				withNodeGroups(3),
			},
			newNumNodeGroups: 5,
			maxRetries:       2,
			expectErr:        true,
			expectErrMsg:     "did not report 5 shards after 3 attempts",
		},
		"describe error is surfaced": {
			errs:             []error{errors.New("boom")},
			newNumNodeGroups: 5,
			maxRetries:       2,
			expectErr:        true,
			expectErrMsg:     "boom",
		},
		"empty response is surfaced rather than panicking": {
			results:          []*elasticache.DescribeReplicationGroupsOutput{{}},
			newNumNodeGroups: 5,
			maxRetries:       0,
			expectErr:        true,
			expectErrMsg:     "no replication groups",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			worker := NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: test.maxRetries,
				},
				&mockRedisClient{
					describeReplicationGroupsResults: test.results,
					describeReplicationGroupsErrs:    test.errs,
				},
				slog.New(&testutil.MockLogHandler{}),
			)

			instance := &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{ServiceID: helpers.RandStr(10)},
					Uuid:    helpers.RandStr(10),
				},
				ClusterID:        "cluster-1",
				NewNumNodeGroups: test.newNumNodeGroups,
			}

			err := worker.verifyIncreasedShardCount(t.Context(), instance)
			if test.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil: the caller treats nil as a completed resize")
				}
				if !strings.Contains(err.Error(), test.expectErrMsg) {
					t.Fatalf("expected error containing %q, got %q", test.expectErrMsg, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
