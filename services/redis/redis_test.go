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
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
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
		"cluster mode enabled sets NumNodeGroups and ReplicasPerNodeGroup instead of NumCacheClusters": {
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
				NumNodeGroups:              3,
				ReplicasPerNodeGroup:       1,
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
				NumNodeGroups:               aws.Int32(int32(3)),
				ReplicasPerNodeGroup:        aws.Int32(int32(1)),
				CacheParameterGroupName:     aws.String("default.valkey.cluster.on"),
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

			job, err := testutil.RequireInsertedTx(test.ctx, t, sqlTx, &ModifyArgs{}, nil)
			if err != nil {
				t.Fatal(err)
			}

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

			job, err := testutil.RequireInsertedTx(test.ctx, t, sqlTx, &DeleteArgs{}, nil)
			if err != nil {
				t.Fatal(err)
			}

			if job.Args.Instance.Uuid != test.instance.Uuid {
				t.Fatal("Did not receive expected instance as delete worker argument")
			}
		})
	}
}

func TestConnectionEndpoint(t *testing.T) {
	testCases := map[string]struct {
		group           elasticacheTypes.ReplicationGroup
		expectedAddress string
		expectedPort    int32
		expectErr       bool
	}{
		"cluster mode disabled uses NodeGroups[0].PrimaryEndpoint": {
			group: elasticacheTypes.ReplicationGroup{
				NodeGroups: []elasticacheTypes.NodeGroup{
					{
						PrimaryEndpoint: &elasticacheTypes.Endpoint{
							Address: aws.String("primary.example.com"),
							Port:    aws.Int32(6379),
						},
					},
				},
			},
			expectedAddress: "primary.example.com",
			expectedPort:    6379,
		},
		"cluster mode enabled uses ConfigurationEndpoint": {
			group: elasticacheTypes.ReplicationGroup{
				ClusterEnabled: aws.Bool(true),
				ConfigurationEndpoint: &elasticacheTypes.Endpoint{
					Address: aws.String("config.example.com"),
					Port:    aws.Int32(6379),
				},
				NodeGroups: []elasticacheTypes.NodeGroup{
					{PrimaryEndpoint: &elasticacheTypes.Endpoint{Address: aws.String("shard-1.example.com"), Port: aws.Int32(6379)}},
					{PrimaryEndpoint: &elasticacheTypes.Endpoint{Address: aws.String("shard-2.example.com"), Port: aws.Int32(6379)}},
				},
			},
			expectedAddress: "config.example.com",
			expectedPort:    6379,
		},
		"cluster mode enabled with missing ConfigurationEndpoint errors": {
			group: elasticacheTypes.ReplicationGroup{
				ClusterEnabled: aws.Bool(true),
			},
			expectErr: true,
		},
		"cluster mode disabled with no node groups errors": {
			group:     elasticacheTypes.ReplicationGroup{},
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			endpoint, err := connectionEndpoint(test.group)
			if test.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if aws.ToString(endpoint.Address) != test.expectedAddress {
				t.Errorf("expected address %q, got %q", test.expectedAddress, aws.ToString(endpoint.Address))
			}
			if aws.ToInt32(endpoint.Port) != test.expectedPort {
				t.Errorf("expected port %d, got %d", test.expectedPort, aws.ToInt32(endpoint.Port))
			}
		})
	}
}
