package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

var (
	ErrPreparingModifyInput      = errors.New("preparing modify input")
	ErrIncreasingReplicaCount    = errors.New("increasing replica count")
	ErrModifyingReplicationGroup = errors.New("modifying replication group")
	ErrIncreasingShardCount      = errors.New("increasing shard count")
)

const (
	ModifyKind = "elasticache-modify"
)

type ModifyArgs struct {
	Instance *RedisInstance `json:"instance"`
}

func (ModifyArgs) Kind() string { return ModifyKind }

type ModifyWorker struct {
	river.WorkerDefaults[ModifyArgs]
	db          *gorm.DB
	settings    *config.Settings
	elasticache ElasticacheClientInterface
	logger      *slog.Logger
}

func NewModifyWorker(
	db *gorm.DB,
	settings *config.Settings,
	elasticache ElasticacheClientInterface,
	logger *slog.Logger,
) *ModifyWorker {
	return &ModifyWorker{
		db:          db,
		settings:    settings,
		elasticache: elasticache,
		logger:      logger,
	}
}

func (w *ModifyWorker) Work(ctx context.Context, job *river.Job[ModifyArgs]) error {
	operation := base.ModifyOp
	i := job.Args.Instance
	errChan := make(chan error, 1)

	go func(ctx context.Context, i *RedisInstance, operation base.Operation) {
		errChan <- w.asyncModifyRedis(ctx, i, operation)
	}(ctx, i, operation)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		if err != nil {
			w.logger.Error("error modifying cluster", "err", err)
			asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, err.Error())
			return river.JobCancel(err)
		}
		return nil
	}
}

func (w *ModifyWorker) asyncModifyRedis(ctx context.Context, i *RedisInstance, operation base.Operation) error {
	params, err := prepareModifyReplicationGroupInput(i)
	if err != nil {
		return common.FmtErr(ErrPreparingModifyInput, err)
	}

	if i.NewNumNodeGroups > 0 {
		err = w.increaseShardCount(ctx, i, operation)
		if err != nil {
			return common.FmtErr(ErrIncreasingShardCount, err)
		}
	}

	if i.NewReplicaCount > 0 {
		err = w.increaseReplicaCount(ctx, i, operation)
		if err != nil {
			return common.FmtErr(ErrIncreasingReplicaCount, err)
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Modifying replication group")

	_, err = w.elasticache.ModifyReplicationGroup(ctx, params)
	if err != nil {
		return common.FmtErr(ErrModifyingReplicationGroup, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Waiting for replication group to finish modifying")

	waiter := elasticache.NewReplicationGroupAvailableWaiter(w.elasticache, func(o *elasticache.ReplicationGroupAvailableWaiterOptions) {
		o.MinDelay = w.settings.PollAwsMinDelay
	})
	err = waiter.Wait(ctx, &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	}, w.settings.PollAwsMaxDuration)
	if err != nil {
		return common.FmtErr(ErrModifyingReplicationGroup, err)
	}

	if err := verifyModifiedEngineAndVersion(ctx, w.elasticache, i); err != nil {
		return common.FmtErr(ErrModifyingReplicationGroup, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished modifying cluster")
	return nil
}

func verifyModifiedEngineAndVersion(ctx context.Context, client ElasticacheClientInterface, i *RedisInstance) error {
	output, err := client.DescribeReplicationGroups(ctx, &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	})
	if err != nil {
		return err
	}
	if output == nil || len(output.ReplicationGroups) == 0 {
		return fmt.Errorf("DescribeReplicationGroups returned no replication groups for %s", i.ClusterID)
	}
	group := output.ReplicationGroups[0]

	if aws.ToString(group.Engine) != i.Engine {
		return fmt.Errorf(
			"replication group %s reports engine %q after modify, expected %q",
			i.ClusterID, aws.ToString(group.Engine), i.Engine,
		)
	}
	return nil
}

func (w *ModifyWorker) increaseReplicaCount(ctx context.Context, i *RedisInstance, operation base.Operation) error {
	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Adding new replica nodes")

	newReplicaCount, err := common.ConvertIntToInt32Safely(i.NewReplicaCount)
	if err != nil {
		return err
	}

	input := &elasticache.IncreaseReplicaCountInput{
		ReplicationGroupId: &i.ClusterID,
		ApplyImmediately:   aws.Bool(true),
	}

	if i.ClusterModeEnabled() {
		nodeGroupIDs, err := w.nodeGroupIDs(ctx, i)
		if err != nil {
			return err
		}
		replicaConfiguration := make([]elasticacheTypes.ConfigureShard, 0, len(nodeGroupIDs))
		for _, nodeGroupID := range nodeGroupIDs {
			replicaConfiguration = append(replicaConfiguration, elasticacheTypes.ConfigureShard{
				NodeGroupId:     aws.String(nodeGroupID),
				NewReplicaCount: newReplicaCount,
			})
		}
		input.ReplicaConfiguration = replicaConfiguration
	} else {
		input.NewReplicaCount = newReplicaCount
	}

	_, err = w.elasticache.IncreaseReplicaCount(ctx, input)
	if err != nil {
		w.logger.Error("error increasing replica count", "err", err)
		return err
	}

	// Wait for replication group to be available
	waiter := elasticache.NewReplicationGroupAvailableWaiter(w.elasticache, func(dawo *elasticache.ReplicationGroupAvailableWaiterOptions) {
		dawo.MinDelay = w.settings.PollAwsMinDelay
	})

	waiterInput := &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	}

	err = waiter.Wait(ctx, waiterInput, w.settings.PollAwsMaxDuration)
	if err != nil {
		return err
	}

	err = w.verifyIncreasedReplicaCount(ctx, i)
	if err != nil {
		return err
	}

	return nil
}

func (w *ModifyWorker) nodeGroupIDs(ctx context.Context, i *RedisInstance) ([]string, error) {
	output, err := w.elasticache.DescribeReplicationGroups(ctx, &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	})
	if err != nil {
		return nil, err
	}
	nodeGroups, err := allNodeGroups(output)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(nodeGroups))
	for _, nodeGroup := range nodeGroups {
		ids = append(ids, aws.ToString(nodeGroup.NodeGroupId))
	}
	return ids, nil
}

func (w *ModifyWorker) increaseShardCount(ctx context.Context, i *RedisInstance, operation base.Operation) error {
	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Adding new shards")

	nodeGroupCount, err := common.ConvertIntToInt32Safely(i.NewNumNodeGroups)
	if err != nil {
		return err
	}

	_, err = w.elasticache.ModifyReplicationGroupShardConfiguration(ctx, &elasticache.ModifyReplicationGroupShardConfigurationInput{
		ReplicationGroupId: &i.ClusterID,
		NodeGroupCount:     nodeGroupCount,
		ApplyImmediately:   aws.Bool(true),
	})
	if err != nil {
		w.logger.Error("error increasing shard count", "err", err)
		return err
	}

	waiter := elasticache.NewReplicationGroupAvailableWaiter(w.elasticache, func(dawo *elasticache.ReplicationGroupAvailableWaiterOptions) {
		dawo.MinDelay = w.settings.PollAwsMinDelay
	})
	waiterInput := &elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: &i.ClusterID,
	}
	err = waiter.Wait(ctx, waiterInput, w.settings.PollAwsMaxDuration)
	if err != nil {
		return err
	}

	return w.verifyIncreasedShardCount(ctx, i)
}

// verifyIncreasedShardCount polls until the replication group reports the
// requested number of shards. As with verifyIncreasedReplicaCount, exhausting
// every attempt without seeing the expected count is a failure: the caller
// treats a nil error as a completed resize.
func (w *ModifyWorker) verifyIncreasedShardCount(ctx context.Context, i *RedisInstance) error {
	var ready bool
	observedCount := 0

	attempts := 1
	maxAttempts := 1 + int(w.settings.PollAwsMaxRetries)

	for !ready && attempts <= maxAttempts {
		w.logger.Info(fmt.Sprintf("verifying shard count. attempt %d of %d", attempts, maxAttempts))
		output, err := w.elasticache.DescribeReplicationGroups(ctx, &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: &i.ClusterID,
		})
		if err != nil {
			return err
		}
		if output == nil || len(output.ReplicationGroups) == 0 {
			return fmt.Errorf("DescribeReplicationGroups returned no replication groups for %s", i.ClusterID)
		}

		observedCount = len(output.ReplicationGroups[0].NodeGroups)
		ready = observedCount == i.NewNumNodeGroups
		if ready {
			break
		}

		attempts += 1
		time.Sleep(w.settings.PollAwsMinDelay)
	}

	if !ready {
		return fmt.Errorf(
			"replication group %s did not report %d shards after %d attempts; last saw %d shards",
			i.ClusterID, i.NewNumNodeGroups, maxAttempts, observedCount,
		)
	}

	return nil
}

// verifyIncreasedReplicaCount polls until every shard in the replication
// group reports the requested number of replica nodes. Exhausting every
// attempt without seeing them is a failure, not a success: the caller marks
// the instance ready on a nil error, so returning nil here would tell the
// tenant the resize completed when it did not.
func (w *ModifyWorker) verifyIncreasedReplicaCount(ctx context.Context, i *RedisInstance) error {
	var nodesReady bool
	var observedStatuses []string
	var observedReplicaCounts []int

	attempts := 1
	maxAttempts := 1 + int(w.settings.PollAwsMaxRetries)

	for !nodesReady && attempts <= maxAttempts {
		w.logger.Info(fmt.Sprintf("verifying replica creation. attempt %d of %d", attempts, maxAttempts))
		output, err := w.elasticache.DescribeReplicationGroups(ctx, &elasticache.DescribeReplicationGroupsInput{
			ReplicationGroupId: &i.ClusterID,
		})

		if err != nil {
			return err
		}

		nodeGroups, err := allNodeGroups(output)
		if err != nil {
			return err
		}

		observedStatuses = make([]string, 0, len(nodeGroups))
		observedReplicaCounts = make([]int, 0, len(nodeGroups))
		nodesReady = true

		for _, nodeGroup := range nodeGroups {
			status := aws.ToString(nodeGroup.Status)

			var replicaNodes []elasticacheTypes.NodeGroupMember
			for _, nodeMember := range nodeGroup.NodeGroupMembers {
				if aws.ToString(nodeMember.CurrentRole) == "replica" {
					replicaNodes = append(replicaNodes, nodeMember)
				}
			}

			observedStatuses = append(observedStatuses, status)
			observedReplicaCounts = append(observedReplicaCounts, len(replicaNodes))

			if status != "available" || len(replicaNodes) != i.NewReplicaCount {
				nodesReady = false
			}
		}

		if nodesReady {
			break
		}

		attempts += 1
		time.Sleep(w.settings.PollAwsMinDelay)
		continue
	}

	if !nodesReady {
		return fmt.Errorf(
			"replication group %s did not report %d replica nodes in every shard after %d attempts; last seen statuses %v with replica counts %v",
			i.ClusterID, i.NewReplicaCount, maxAttempts, observedStatuses, observedReplicaCounts,
		)
	}

	return nil
}

// allNodeGroups returns every shard the replication group reports,
// converting a short or empty response into an error. Indexing it directly
// would panic inside the worker instead of failing the job with a
// diagnosable message. For a cluster-mode-disabled replication group this
// is always a single-element slice.
func allNodeGroups(output *elasticache.DescribeReplicationGroupsOutput) ([]elasticacheTypes.NodeGroup, error) {
	if output == nil || len(output.ReplicationGroups) == 0 {
		return nil, errors.New("DescribeReplicationGroups returned no replication groups")
	}
	nodeGroups := output.ReplicationGroups[0].NodeGroups
	if len(nodeGroups) == 0 {
		return nil, errors.New("DescribeReplicationGroups returned a replication group with no node groups")
	}
	return nodeGroups, nil
}

func prepareModifyReplicationGroupInput(i *RedisInstance) (*elasticache.ModifyReplicationGroupInput, error) {
	securityGroups := []string{i.SecGroup}

	snapshotRetentionLimit, err := common.ConvertIntToInt32Safely(i.SnapshotRetentionLimit)
	if err != nil {
		return nil, err
	}

	// Standard parameters
	params := &elasticache.ModifyReplicationGroupInput{
		ReplicationGroupDescription: aws.String(i.Description),
		AutomaticFailoverEnabled:    aws.Bool(i.AutomaticFailoverEnabled),
		ReplicationGroupId:          aws.String(i.ClusterID),
		CacheNodeType:               aws.String(i.CacheNodeType),
		SecurityGroupIds:            securityGroups,
		Engine:                      aws.String(i.Engine),
		PreferredMaintenanceWindow:  aws.String(i.PreferredMaintenanceWindow),
		SnapshotWindow:              aws.String(i.SnapshotWindow),
		SnapshotRetentionLimit:      snapshotRetentionLimit,
		ApplyImmediately:            aws.Bool(true),
	}
	if i.EngineVersion != "" {
		params.EngineVersion = aws.String(i.EngineVersion)
	}
	return params, nil
}
