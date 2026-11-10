package elasticache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/cloud-gov/aws-broker/services/redis"
	"gorm.io/gorm"
)

// Statuses meaning a service update is outstanding (not yet applied/started).
var outstandingUpdateActionStatuses = []elasticacheTypes.UpdateActionStatus{
	elasticacheTypes.UpdateActionStatusNotApplied,
	elasticacheTypes.UpdateActionStatusWaitingToStart,
}

// AWS's hard limit on replication group IDs per API call.
const maxReplicationGroupIDsPerRequest = 20

// DefaultApplyBatchSize bounds how many replication groups fail over/reboot at once.
const DefaultApplyBatchSize = 5

// DefaultStaggerDelay spaces out batches so they don't all hit at once.
const DefaultStaggerDelay = 5 * time.Minute

// ApplyOptions controls batching, pacing, and dry-run preview.
type ApplyOptions struct {
	// BatchSize is replication groups per apply call, clamped to [1, 20].
	BatchSize int
	// StaggerDelay is the pause between batches (not before the first).
	StaggerDelay time.Duration
	// DryRun logs the plan without applying anything.
	DryRun bool
	// Sleep defaults to time.Sleep; overridable in tests.
	Sleep func(time.Duration)
}

// DefaultApplyOptions returns the recommended production settings.
func DefaultApplyOptions() ApplyOptions {
	return ApplyOptions{
		BatchSize:    DefaultApplyBatchSize,
		StaggerDelay: DefaultStaggerDelay,
		DryRun:       false,
		Sleep:        time.Sleep,
	}
}

// normalized fills in safe defaults for missing/invalid fields.
func (opts ApplyOptions) normalized() ApplyOptions {
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultApplyBatchSize
	}
	if opts.BatchSize > maxReplicationGroupIDsPerRequest {
		opts.BatchSize = maxReplicationGroupIDsPerRequest
	}
	if opts.Sleep == nil {
		opts.Sleep = time.Sleep
	}
	return opts
}

// chunkStrings splits ids into slices of at most size each.
func chunkStrings(ids []string, size int) [][]string {
	var chunks [][]string
	for i := 0; i < len(ids); i += size {
		end := i + size
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[i:end])
	}
	return chunks
}

// getBrokeredReplicationGroupIDs returns every Redis/Valkey replication
// group ID this broker manages, across all orgs/spaces.
func getBrokeredReplicationGroupIDs(db *gorm.DB) ([]string, error) {
	rows, err := db.Model(&redis.RedisInstance{}).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var replicationGroupIDs []string
	for rows.Next() {
		var redisInstance redis.RedisInstance
		db.ScanRows(rows, &redisInstance)

		if redisInstance.ClusterID == "" {
			continue
		}
		replicationGroupIDs = append(replicationGroupIDs, redisInstance.ClusterID)
	}

	return replicationGroupIDs, nil
}

// outstandingUpdatesByName maps a service update name to the replication
// groups that have it outstanding.
type outstandingUpdatesByName map[string][]string

func isOutstandingUpdateActionStatus(status elasticacheTypes.UpdateActionStatus) bool {
	for _, s := range outstandingUpdateActionStatuses {
		if status == s {
			return true
		}
	}
	return false
}

// describeOutstandingServiceUpdates returns outstanding service updates for
// replicationGroupIDs, grouped by update name.
func describeOutstandingServiceUpdates(elasticacheClient ElasticacheClientInterface, replicationGroupIDs []string) (outstandingUpdatesByName, error) {
	updates := outstandingUpdatesByName{}

	for _, chunk := range chunkStrings(replicationGroupIDs, maxReplicationGroupIDsPerRequest) {
		input := &elasticache.DescribeUpdateActionsInput{
			ReplicationGroupIds: chunk,
		}

		for {
			output, err := elasticacheClient.DescribeUpdateActions(context.TODO(), input)
			if err != nil {
				return nil, fmt.Errorf("could not describe update actions: %s", err)
			}

			for _, action := range output.UpdateActions {
				if action.ServiceUpdateName == nil || action.ReplicationGroupId == nil {
					continue
				}
				if !isOutstandingUpdateActionStatus(action.UpdateActionStatus) {
					continue
				}
				if action.ServiceUpdateStatus != elasticacheTypes.ServiceUpdateStatusAvailable {
					continue
				}
				name := *action.ServiceUpdateName
				updates[name] = append(updates[name], *action.ReplicationGroupId)
			}

			if output.Marker == nil || *output.Marker == "" {
				break
			}
			input.Marker = output.Marker
		}
	}

	return updates, nil
}

// applyBatch is one planned BatchApplyUpdateAction call.
type applyBatch struct {
	ServiceUpdateName   string
	ReplicationGroupIDs []string
}

// planApplyBatches flattens outstandingUpdates into batchSize-sized batches.
func planApplyBatches(outstandingUpdates outstandingUpdatesByName, batchSize int) []applyBatch {
	var batches []applyBatch
	for serviceUpdateName, groupIDs := range outstandingUpdates {
		for _, chunk := range chunkStrings(groupIDs, batchSize) {
			batches = append(batches, applyBatch{
				ServiceUpdateName:   serviceUpdateName,
				ReplicationGroupIDs: chunk,
			})
		}
	}
	return batches
}

// applyOneBatch issues a single BatchApplyUpdateAction call for batch.
func applyOneBatch(elasticacheClient ElasticacheClientInterface, batch applyBatch) error {
	log.Printf("applying elasticache service update %s to replication groups: %v", batch.ServiceUpdateName, batch.ReplicationGroupIDs)

	output, err := elasticacheClient.BatchApplyUpdateAction(context.TODO(), &elasticache.BatchApplyUpdateActionInput{
		ServiceUpdateName:   aws.String(batch.ServiceUpdateName),
		ReplicationGroupIds: batch.ReplicationGroupIDs,
	})
	if err != nil {
		return fmt.Errorf("error applying service update %s to replication groups %v: %s", batch.ServiceUpdateName, batch.ReplicationGroupIDs, err)
	}

	var errs error
	for _, unprocessed := range output.UnprocessedUpdateActions {
		replicationGroupID := ""
		if unprocessed.ReplicationGroupId != nil {
			replicationGroupID = *unprocessed.ReplicationGroupId
		}
		errorMessage := ""
		if unprocessed.ErrorMessage != nil {
			errorMessage = *unprocessed.ErrorMessage
		}
		log.Printf("service update %s was not processed for replication group %s: %s", batch.ServiceUpdateName, replicationGroupID, errorMessage)
		errs = errors.Join(errs, fmt.Errorf("service update %s not processed for replication group %s: %s", batch.ServiceUpdateName, replicationGroupID, errorMessage))
	}

	for _, processed := range output.ProcessedUpdateActions {
		replicationGroupID := ""
		if processed.ReplicationGroupId != nil {
			replicationGroupID = *processed.ReplicationGroupId
		}
		log.Printf("service update %s is now %s for replication group %s", batch.ServiceUpdateName, processed.UpdateActionStatus, replicationGroupID)
	}

	return errs
}

// ApplyElasticacheServiceUpdates applies outstanding ElastiCache service
// updates to every Redis/Valkey replication group this broker manages,
// across all orgs/spaces. Updates are applied in batches of opts.BatchSize,
// paced opts.StaggerDelay apart, to bound simultaneous failovers/reboots.
// If opts.DryRun is true, it only logs the plan without applying anything.
func ApplyElasticacheServiceUpdates(db *gorm.DB, elasticacheClient ElasticacheClientInterface, opts ApplyOptions) error {
	opts = opts.normalized()

	replicationGroupIDs, err := getBrokeredReplicationGroupIDs(db)
	if err != nil {
		return fmt.Errorf("could not list broker-managed elasticache replication groups: %s", err)
	}

	if len(replicationGroupIDs) == 0 {
		log.Printf("no broker-managed elasticache replication groups found, nothing to do")
		return nil
	}

	outstandingUpdates, err := describeOutstandingServiceUpdates(elasticacheClient, replicationGroupIDs)
	if err != nil {
		return err
	}

	if len(outstandingUpdates) == 0 {
		log.Printf("no outstanding elasticache service updates found for %d broker-managed replication groups", len(replicationGroupIDs))
		return nil
	}

	batches := planApplyBatches(outstandingUpdates, opts.BatchSize)

	if opts.DryRun {
		log.Printf("[dry-run] would apply %d outstanding service update(s) across %d replication group(s) in %d batch(es) of up to %d, staggered %s apart:", len(outstandingUpdates), len(replicationGroupIDs), len(batches), opts.BatchSize, opts.StaggerDelay)
		for i, batch := range batches {
			log.Printf("[dry-run] batch %d/%d: service update %s -> replication groups %v", i+1, len(batches), batch.ServiceUpdateName, batch.ReplicationGroupIDs)
		}
		return nil
	}

	var errs error
	for i, batch := range batches {
		if i > 0 && opts.StaggerDelay > 0 {
			opts.Sleep(opts.StaggerDelay)
		}
		if err := applyOneBatch(elasticacheClient, batch); err != nil {
			errs = errors.Join(errs, err)
			continue
		}
	}

	return errs
}
