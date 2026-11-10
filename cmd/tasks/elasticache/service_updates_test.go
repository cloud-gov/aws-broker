package elasticache

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/services/redis"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
)

type mockElasticacheServiceUpdatesClient struct {
	describeUpdateActionsOutput *elasticache.DescribeUpdateActionsOutput
	describeUpdateActionsErr    error
	describeUpdateActionsCalls  []*elasticache.DescribeUpdateActionsInput

	batchApplyOutputs map[string]*elasticache.BatchApplyUpdateActionOutput
	batchApplyErrs    map[string]error
	batchApplyCalls   []*elasticache.BatchApplyUpdateActionInput
}

func (m *mockElasticacheServiceUpdatesClient) AddTagsToResource(ctx context.Context, params *elasticache.AddTagsToResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.AddTagsToResourceOutput, error) {
	return nil, nil
}

func (m *mockElasticacheServiceUpdatesClient) DescribeReplicationGroups(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error) {
	return nil, nil
}

func (m *mockElasticacheServiceUpdatesClient) ListTagsForResource(ctx context.Context, params *elasticache.ListTagsForResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.ListTagsForResourceOutput, error) {
	return nil, nil
}

func (m *mockElasticacheServiceUpdatesClient) DescribeUpdateActions(ctx context.Context, params *elasticache.DescribeUpdateActionsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeUpdateActionsOutput, error) {
	m.describeUpdateActionsCalls = append(m.describeUpdateActionsCalls, params)
	if m.describeUpdateActionsErr != nil {
		return nil, m.describeUpdateActionsErr
	}
	if m.describeUpdateActionsOutput == nil {
		return &elasticache.DescribeUpdateActionsOutput{}, nil
	}
	return m.describeUpdateActionsOutput, nil
}

func (m *mockElasticacheServiceUpdatesClient) BatchApplyUpdateAction(ctx context.Context, params *elasticache.BatchApplyUpdateActionInput, optFns ...func(*elasticache.Options)) (*elasticache.BatchApplyUpdateActionOutput, error) {
	m.batchApplyCalls = append(m.batchApplyCalls, params)

	name := ""
	if params.ServiceUpdateName != nil {
		name = *params.ServiceUpdateName
	}

	if err, ok := m.batchApplyErrs[name]; ok && err != nil {
		return nil, err
	}

	if output, ok := m.batchApplyOutputs[name]; ok {
		return output, nil
	}

	return &elasticache.BatchApplyUpdateActionOutput{}, nil
}

func seedRedisInstances(t *testing.T, db *gorm.DB, clusterIDs ...string) {
	t.Helper()
	for i, clusterID := range clusterIDs {
		instance := &redis.RedisInstance{
			Instance:  base.Instance{Uuid: clusterID + "-uuid"},
			ClusterID: clusterID,
		}
		if err := db.Create(instance).Error; err != nil {
			t.Fatalf("could not seed redis instance %d: %s", i, err)
		}
	}
}

func testDBInit(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := testutil.TestDbInit()
	if err != nil {
		t.Fatalf("could not init test db: %s", err)
	}
	if err := db.AutoMigrate(&redis.RedisInstance{}, &base.Instance{}); err != nil {
		t.Fatalf("could not migrate test db: %s", err)
	}
	return db
}

// testOptions returns ApplyOptions tuned for fast, deterministic tests: a
// no-op Sleep (so staggering doesn't slow down the test suite) and an
// explicit BatchSize/StaggerDelay per test.
func testOptions(batchSize int, staggerDelay time.Duration) ApplyOptions {
	return ApplyOptions{
		BatchSize:    batchSize,
		StaggerDelay: staggerDelay,
		Sleep:        func(time.Duration) {},
	}
}

func manyClusterIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("cluster-%d", i)
	}
	return ids
}

func TestGetBrokeredReplicationGroupIDs(t *testing.T) {
	db := testDBInit(t)
	seedRedisInstances(t, db, "cluster-1", "cluster-2", "cluster-3")

	ids, err := getBrokeredReplicationGroupIDs(db)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 3 {
		t.Fatalf("expected 3 replication group ids, got %d: %v", len(ids), ids)
	}
}

func TestChunkStrings(t *testing.T) {
	testCases := map[string]struct {
		ids        []string
		size       int
		wantChunks [][]string
	}{
		"empty": {
			ids:        nil,
			size:       20,
			wantChunks: nil,
		},
		"fewer than one chunk": {
			ids:        []string{"a", "b", "c"},
			size:       20,
			wantChunks: [][]string{{"a", "b", "c"}},
		},
		"exactly one chunk": {
			ids:        []string{"a", "b"},
			size:       2,
			wantChunks: [][]string{{"a", "b"}},
		},
		"more than one chunk, uneven last chunk": {
			ids:        []string{"a", "b", "c", "d", "e"},
			size:       2,
			wantChunks: [][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			got := chunkStrings(test.ids, test.size)
			if len(got) != len(test.wantChunks) {
				t.Fatalf("expected %d chunks, got %d: %v", len(test.wantChunks), len(got), got)
			}
			for i, wantChunk := range test.wantChunks {
				if len(got[i]) != len(wantChunk) {
					t.Fatalf("chunk %d: expected %v, got %v", i, wantChunk, got[i])
				}
			}
		})
	}
}

func TestApplyOptionsNormalized(t *testing.T) {
	testCases := map[string]struct {
		opts          ApplyOptions
		wantBatchSize int
		wantSleepNil  bool
	}{
		"zero value gets safe defaults": {
			opts:          ApplyOptions{},
			wantBatchSize: DefaultApplyBatchSize,
		},
		"negative batch size falls back to default": {
			opts:          ApplyOptions{BatchSize: -5},
			wantBatchSize: DefaultApplyBatchSize,
		},
		"batch size above the AWS limit is clamped": {
			opts:          ApplyOptions{BatchSize: 1000},
			wantBatchSize: maxReplicationGroupIDsPerRequest,
		},
		"valid batch size is preserved": {
			opts:          ApplyOptions{BatchSize: 7},
			wantBatchSize: 7,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			got := test.opts.normalized()
			if got.BatchSize != test.wantBatchSize {
				t.Fatalf("expected BatchSize %d, got %d", test.wantBatchSize, got.BatchSize)
			}
			if got.Sleep == nil {
				t.Fatal("expected normalized Sleep to never be nil")
			}
		})
	}
}

func TestDescribeOutstandingServiceUpdates(t *testing.T) {
	testCases := map[string]struct {
		mockClient    *mockElasticacheServiceUpdatesClient
		groupIDs      []string
		expectUpdates outstandingUpdatesByName
		expectErr     bool
	}{
		"no replication groups": {
			mockClient:    &mockElasticacheServiceUpdatesClient{},
			groupIDs:      nil,
			expectUpdates: outstandingUpdatesByName{},
		},
		"single outstanding update for multiple groups": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
					UpdateActions: []elasticacheTypes.UpdateAction{
						{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
						{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-2"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					},
				},
			},
			groupIDs: []string{"cluster-1", "cluster-2"},
			expectUpdates: outstandingUpdatesByName{
				"update-1": {"cluster-1", "cluster-2"},
			},
		},
		"multiple distinct updates grouped separately": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
					UpdateActions: []elasticacheTypes.UpdateAction{
						{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
						{ServiceUpdateName: aws.String("update-2"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
						{ServiceUpdateName: aws.String("update-2"), ReplicationGroupId: aws.String("cluster-2"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					},
				},
			},
			groupIDs: []string{"cluster-1", "cluster-2"},
			expectUpdates: outstandingUpdatesByName{
				"update-1": {"cluster-1"},
				"update-2": {"cluster-1", "cluster-2"},
			},
		},
		"describe error": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				describeUpdateActionsErr: errors.New("boom"),
			},
			groupIDs:  []string{"cluster-1"},
			expectErr: true,
		},
		"non-outstanding statuses are filtered out client-side": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
					UpdateActions: []elasticacheTypes.UpdateAction{
						{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
						{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-2"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusWaitingToStart},
						{ServiceUpdateName: aws.String("update-2"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusComplete},
						{ServiceUpdateName: aws.String("update-3"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusInProgress},
					},
				},
			},
			groupIDs: []string{"cluster-1", "cluster-2"},
			expectUpdates: outstandingUpdatesByName{
				"update-1": {"cluster-1", "cluster-2"},
			},
		},
		"more than 20 replication groups is chunked across multiple calls": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
					UpdateActions: []elasticacheTypes.UpdateAction{
						{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-0"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					},
				},
			},
			groupIDs: manyClusterIDs(45),
			expectUpdates: outstandingUpdatesByName{
				// Each of the 3 chunk calls returns the same mocked single
				// action, so the real assertion here (done separately
				// below, not via expectUpdates) is the call count.
				"update-1": {"cluster-0", "cluster-0", "cluster-0"},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			updates, err := describeOutstandingServiceUpdates(test.mockClient, test.groupIDs)
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error, got nil")
			}
			if test.expectErr {
				return
			}

			if len(updates) != len(test.expectUpdates) {
				t.Fatalf("expected %d distinct updates, got %d: %v", len(test.expectUpdates), len(updates), updates)
			}
			for wantName, wantGroups := range test.expectUpdates {
				gotGroups, ok := updates[wantName]
				if !ok {
					t.Fatalf("expected update %s to be present", wantName)
				}
				if len(gotGroups) != len(wantGroups) {
					t.Fatalf("expected update %s to have groups %v, got %v", wantName, wantGroups, gotGroups)
				}
			}

			if name == "more than 20 replication groups is chunked across multiple calls" {
				if len(test.mockClient.describeUpdateActionsCalls) != 3 {
					t.Fatalf("expected 45 ids to be chunked into 3 DescribeUpdateActions calls (20/20/5), got %d", len(test.mockClient.describeUpdateActionsCalls))
				}
				for i, call := range test.mockClient.describeUpdateActionsCalls {
					if len(call.ReplicationGroupIds) > maxReplicationGroupIDsPerRequest {
						t.Fatalf("call %d had %d replication group ids, exceeding the AWS limit of %d", i, len(call.ReplicationGroupIds), maxReplicationGroupIDsPerRequest)
					}
				}
			}

			// Regression guard: AWS rejects DescribeUpdateActions with
			// InvalidParameterCombination if UpdateActionStatus is set
			// without also setting ServiceUpdateName. Discovery here is
			// name-agnostic, so UpdateActionStatus must never be sent on
			// the request; filtering happens client-side instead.
			for i, call := range test.mockClient.describeUpdateActionsCalls {
				if call.UpdateActionStatus != nil {
					t.Fatalf("call %d must not set UpdateActionStatus on the request (AWS requires ServiceUpdateName alongside it); got %v", i, call.UpdateActionStatus)
				}
			}
		})
	}
}

func TestPlanApplyBatches(t *testing.T) {
	testCases := map[string]struct {
		updates       outstandingUpdatesByName
		batchSize     int
		wantNumBatch  int
		wantMaxPerLen int
	}{
		"single update, under batch size": {
			updates:       outstandingUpdatesByName{"update-1": {"cluster-1", "cluster-2"}},
			batchSize:     5,
			wantNumBatch:  1,
			wantMaxPerLen: 2,
		},
		"single update, exactly matching several batches": {
			updates:       outstandingUpdatesByName{"update-1": manyClusterIDs(12)},
			batchSize:     5,
			wantNumBatch:  3, // 5, 5, 2
			wantMaxPerLen: 5,
		},
		"multiple updates, each batched independently": {
			updates: outstandingUpdatesByName{
				"update-1": manyClusterIDs(7),
				"update-2": manyClusterIDs(3),
			},
			batchSize:     5,
			wantNumBatch:  3, // update-1: 5+2, update-2: 3
			wantMaxPerLen: 5,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			batches := planApplyBatches(test.updates, test.batchSize)
			if len(batches) != test.wantNumBatch {
				t.Fatalf("expected %d batches, got %d: %+v", test.wantNumBatch, len(batches), batches)
			}
			for _, batch := range batches {
				if len(batch.ReplicationGroupIDs) > test.wantMaxPerLen {
					t.Fatalf("batch exceeded expected max size %d: %+v", test.wantMaxPerLen, batch)
				}
				if batch.ServiceUpdateName == "" {
					t.Fatal("batch is missing a ServiceUpdateName")
				}
			}
		})
	}
}

func TestApplyOneBatch(t *testing.T) {
	testCases := map[string]struct {
		mockClient *mockElasticacheServiceUpdatesClient
		batch      applyBatch
		expectErr  bool
	}{
		"success, all processed": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				batchApplyOutputs: map[string]*elasticache.BatchApplyUpdateActionOutput{
					"update-1": {
						ProcessedUpdateActions: []elasticacheTypes.ProcessedUpdateAction{
							{ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusScheduling},
						},
					},
				},
			},
			batch: applyBatch{ServiceUpdateName: "update-1", ReplicationGroupIDs: []string{"cluster-1"}},
		},
		"partial failure, some unprocessed": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				batchApplyOutputs: map[string]*elasticache.BatchApplyUpdateActionOutput{
					"update-1": {
						UnprocessedUpdateActions: []elasticacheTypes.UnprocessedUpdateAction{
							{ReplicationGroupId: aws.String("cluster-2"), ErrorMessage: aws.String("not eligible")},
						},
					},
				},
			},
			batch:     applyBatch{ServiceUpdateName: "update-1", ReplicationGroupIDs: []string{"cluster-1", "cluster-2"}},
			expectErr: true,
		},
		"api call error": {
			mockClient: &mockElasticacheServiceUpdatesClient{
				batchApplyErrs: map[string]error{
					"update-1": errors.New("boom"),
				},
			},
			batch:     applyBatch{ServiceUpdateName: "update-1", ReplicationGroupIDs: []string{"cluster-1"}},
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := applyOneBatch(test.mockClient, test.batch)
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error, got nil")
			}

			if len(test.mockClient.batchApplyCalls) != 1 {
				t.Fatalf("expected exactly 1 BatchApplyUpdateAction call, got %d", len(test.mockClient.batchApplyCalls))
			}
			call := test.mockClient.batchApplyCalls[0]
			if call.ServiceUpdateName == nil || *call.ServiceUpdateName != test.batch.ServiceUpdateName {
				t.Fatalf("expected ServiceUpdateName %s, got %v", test.batch.ServiceUpdateName, call.ServiceUpdateName)
			}
			if len(call.ReplicationGroupIds) != len(test.batch.ReplicationGroupIDs) {
				t.Fatalf("expected %d replication group ids in call, got %d", len(test.batch.ReplicationGroupIDs), len(call.ReplicationGroupIds))
			}
		})
	}
}

func TestApplyElasticacheServiceUpdates(t *testing.T) {
	t.Run("no broker-managed instances", func(t *testing.T) {
		db := testDBInit(t)
		mockClient := &mockElasticacheServiceUpdatesClient{}

		if err := ApplyElasticacheServiceUpdates(db, mockClient, testOptions(5, 0)); err != nil {
			t.Fatal(err)
		}
		if len(mockClient.describeUpdateActionsCalls) != 0 {
			t.Fatalf("expected DescribeUpdateActions not to be called when there are no instances")
		}
	})

	t.Run("no outstanding updates", func(t *testing.T) {
		db := testDBInit(t)
		seedRedisInstances(t, db, "cluster-1")
		mockClient := &mockElasticacheServiceUpdatesClient{}

		if err := ApplyElasticacheServiceUpdates(db, mockClient, testOptions(5, 0)); err != nil {
			t.Fatal(err)
		}
		if len(mockClient.batchApplyCalls) != 0 {
			t.Fatalf("expected no BatchApplyUpdateAction calls when there are no outstanding updates")
		}
	})

	t.Run("applies each distinct outstanding update once when under batch size", func(t *testing.T) {
		db := testDBInit(t)
		seedRedisInstances(t, db, "cluster-1", "cluster-2")

		mockClient := &mockElasticacheServiceUpdatesClient{
			describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
				UpdateActions: []elasticacheTypes.UpdateAction{
					{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-2"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					{ServiceUpdateName: aws.String("update-2"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
				},
			},
		}

		if err := ApplyElasticacheServiceUpdates(db, mockClient, testOptions(5, 0)); err != nil {
			t.Fatal(err)
		}

		if len(mockClient.batchApplyCalls) != 2 {
			t.Fatalf("expected 2 BatchApplyUpdateAction calls (one per distinct update name), got %d", len(mockClient.batchApplyCalls))
		}
	})

	t.Run("one failing batch does not block the others", func(t *testing.T) {
		db := testDBInit(t)
		seedRedisInstances(t, db, "cluster-1")

		mockClient := &mockElasticacheServiceUpdatesClient{
			describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
				UpdateActions: []elasticacheTypes.UpdateAction{
					{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					{ServiceUpdateName: aws.String("update-2"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
				},
			},
			batchApplyErrs: map[string]error{
				"update-1": errors.New("boom"),
			},
		}

		err := ApplyElasticacheServiceUpdates(db, mockClient, testOptions(5, 0))
		if err == nil {
			t.Fatal("expected an error because update-1 failed")
		}
		if len(mockClient.batchApplyCalls) != 2 {
			t.Fatalf("expected both update-1 and update-2 to be attempted, got %d calls", len(mockClient.batchApplyCalls))
		}
	})

	t.Run("covers every instance across every org and space in one run, chunking past the 20-id AWS limit for discovery", func(t *testing.T) {
		db := testDBInit(t)

		// Simulate instances spread across many different CF orgs/spaces:
		// the broker DB query has no org/space filter, so all of them are
		// discovered in a single job run. Seed 45 instances (> 2x the
		// AWS 20-id-per-call limit) to prove chunking covers the full
		// fleet rather than silently dropping instances past the first 20.
		clusterIDs := manyClusterIDs(45)
		for i, clusterID := range clusterIDs {
			instance := &redis.RedisInstance{
				Instance: base.Instance{
					Uuid: clusterID + "-uuid",
					Request: request.Request{
						OrganizationGUID: fmt.Sprintf("org-%d", i%5),
						SpaceGUID:        fmt.Sprintf("space-%d", i%3),
					},
				},
				ClusterID: clusterID,
			}
			if err := db.Create(instance).Error; err != nil {
				t.Fatalf("could not seed redis instance %d: %s", i, err)
			}
		}

		mockClient := &mockElasticacheServiceUpdatesClient{
			describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
				UpdateActions: []elasticacheTypes.UpdateAction{
					{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-0"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
				},
			},
		}

		if err := ApplyElasticacheServiceUpdates(db, mockClient, testOptions(5, 0)); err != nil {
			t.Fatal(err)
		}

		// 45 ids chunked by the AWS discovery limit (20) => 3
		// DescribeUpdateActions calls, covering every org/space's
		// instances in this one job run.
		if len(mockClient.describeUpdateActionsCalls) != 3 {
			t.Fatalf("expected discovery to chunk 45 instances into 3 calls, got %d", len(mockClient.describeUpdateActionsCalls))
		}
		totalDescribed := 0
		for _, call := range mockClient.describeUpdateActionsCalls {
			totalDescribed += len(call.ReplicationGroupIds)
		}
		if totalDescribed != 45 {
			t.Fatalf("expected all 45 instances to be covered across chunked calls, got %d", totalDescribed)
		}
	})

	t.Run("applies in small batches, not one burst, to bound simultaneous failovers", func(t *testing.T) {
		db := testDBInit(t)
		clusterIDs := manyClusterIDs(12)
		seedRedisInstances(t, db, clusterIDs...)

		var updateActions []elasticacheTypes.UpdateAction
		for _, id := range clusterIDs {
			updateActions = append(updateActions, elasticacheTypes.UpdateAction{
				ServiceUpdateName:  aws.String("update-1"),
				ReplicationGroupId: aws.String(id),
				UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied,
			})
		}

		mockClient := &mockElasticacheServiceUpdatesClient{
			describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
				UpdateActions: updateActions,
			},
		}

		// BatchSize of 5 against 12 replication groups should produce 3
		// BatchApplyUpdateAction calls (5, 5, 2), not 1 call covering all
		// 12 at once.
		if err := ApplyElasticacheServiceUpdates(db, mockClient, testOptions(5, time.Millisecond)); err != nil {
			t.Fatal(err)
		}

		if len(mockClient.batchApplyCalls) != 3 {
			t.Fatalf("expected 3 batched BatchApplyUpdateAction calls (5/5/2), got %d", len(mockClient.batchApplyCalls))
		}
		for _, call := range mockClient.batchApplyCalls {
			if len(call.ReplicationGroupIds) > 5 {
				t.Fatalf("expected no call to exceed the configured batch size of 5, got %d replication groups in one call", len(call.ReplicationGroupIds))
			}
		}
	})

	t.Run("dry run discovers but never calls BatchApplyUpdateAction", func(t *testing.T) {
		db := testDBInit(t)
		seedRedisInstances(t, db, "cluster-1", "cluster-2")

		mockClient := &mockElasticacheServiceUpdatesClient{
			describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
				UpdateActions: []elasticacheTypes.UpdateAction{
					{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-1"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
					{ServiceUpdateName: aws.String("update-1"), ReplicationGroupId: aws.String("cluster-2"), UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied},
				},
			},
		}

		opts := testOptions(5, 0)
		opts.DryRun = true
		opts.Sleep = func(time.Duration) {
			t.Fatal("dry run must never sleep")
		}

		if err := ApplyElasticacheServiceUpdates(db, mockClient, opts); err != nil {
			t.Fatal(err)
		}

		if len(mockClient.describeUpdateActionsCalls) == 0 {
			t.Fatal("expected dry run to still discover outstanding updates")
		}
		if len(mockClient.batchApplyCalls) != 0 {
			t.Fatalf("expected dry run to never call BatchApplyUpdateAction, got %d calls", len(mockClient.batchApplyCalls))
		}
	})

	t.Run("staggers batches with the configured delay between calls, not before the first", func(t *testing.T) {
		db := testDBInit(t)
		clusterIDs := manyClusterIDs(10)
		seedRedisInstances(t, db, clusterIDs...)

		var updateActions []elasticacheTypes.UpdateAction
		for _, id := range clusterIDs {
			updateActions = append(updateActions, elasticacheTypes.UpdateAction{
				ServiceUpdateName:  aws.String("update-1"),
				ReplicationGroupId: aws.String(id),
				UpdateActionStatus: elasticacheTypes.UpdateActionStatusNotApplied,
			})
		}

		mockClient := &mockElasticacheServiceUpdatesClient{
			describeUpdateActionsOutput: &elasticache.DescribeUpdateActionsOutput{
				UpdateActions: updateActions,
			},
		}

		var sleptDurations []time.Duration
		opts := ApplyOptions{
			BatchSize:    5,
			StaggerDelay: 42 * time.Millisecond,
			Sleep: func(d time.Duration) {
				sleptDurations = append(sleptDurations, d)
			},
		}

		if err := ApplyElasticacheServiceUpdates(db, mockClient, opts); err != nil {
			t.Fatal(err)
		}

		// 10 ids / batch size 5 = 2 batches => exactly 1 stagger pause
		// (before the 2nd batch, not before the 1st).
		if len(sleptDurations) != 1 {
			t.Fatalf("expected exactly 1 stagger pause between 2 batches, got %d", len(sleptDurations))
		}
		if sleptDurations[0] != 42*time.Millisecond {
			t.Fatalf("expected stagger delay of 42ms, got %s", sleptDurations[0])
		}
	})
}
