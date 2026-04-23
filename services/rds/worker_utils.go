package rds

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"gorm.io/gorm"
)

type WorkerUtils struct {
	db       *gorm.DB
	rds      RDSClientInterface
	settings *config.Settings
	logger   *slog.Logger
}

func (w *WorkerUtils) waitForDbReady(
	ctx context.Context,
	operation base.Operation,
	i *RDSInstance,
	database string,
) error {
	w.logger.Debug(fmt.Sprintf("Waiting for DB instance %s to be available", database))

	// Create a waiter
	waiter := rds.NewDBInstanceAvailableWaiter(w.rds, func(dawo *rds.DBInstanceAvailableWaiterOptions) {
		dawo.MinDelay = w.settings.PollAwsMinDelay
		dawo.LogWaitAttempts = true
	})

	// Define the waiting strategy
	maxWaitTime := getPollAwsMaxWaitTime(i.AllocatedStorage, w.settings.PollAwsMaxDuration)

	waiterInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	}
	err := waiter.Wait(ctx, waiterInput, maxWaitTime)

	if err != nil {
		updateErr := jobs.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Failed waiting for database to become available: %s", err))
		if updateErr != nil {
			err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
		}
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func (w *WorkerUtils) updateDBTags(ctx context.Context, i *RDSInstance, dbInstanceARN string) error {
	_, err := w.rds.AddTagsToResource(ctx, &rds.AddTagsToResourceInput{
		ResourceName: aws.String(dbInstanceARN),
		Tags:         ConvertTagsToRDSTags(i.getTags()),
	})
	return err
}

func (w *WorkerUtils) createDBReadReplica(
	ctx context.Context,
	i *RDSInstance,
	plan *catalog.RDSPlan,
) (*rds.CreateDBInstanceReadReplicaOutput, error) {
	var err error

	rdsTags := ConvertTagsToRDSTags(i.getTags())
	createReadReplicaParams := &rds.CreateDBInstanceReadReplicaInput{
		AutoMinorVersionUpgrade:    aws.Bool(true),
		DBInstanceIdentifier:       &i.ReplicaDatabase,
		SourceDBInstanceIdentifier: &i.Database,
		MultiAZ:                    &plan.Redundant,
		PubliclyAccessible:         aws.Bool(w.settings.PubliclyAccessibleFeature && i.PubliclyAccessible),
		StorageType:                aws.String(i.StorageType),
		Tags:                       rdsTags,
		VpcSecurityGroupIds: []string{
			i.SecGroup,
		},
	}

	var createDbInstanceReplicaSuccess bool
	var createDbInstanceReadReplicaOutput *rds.CreateDBInstanceReadReplicaOutput

	attempts := 1
	maxRetries := getPollAwsMaxRetries(i.AllocatedStorage, w.settings.PollAwsMaxRetries)
	// max attempts = initial attempt + retries
	maxAttempts := 1 + maxRetries

	for !createDbInstanceReplicaSuccess && attempts <= maxAttempts {
		w.logger.Info(fmt.Sprintf("attempting replica creation. attempt %d of %d", attempts, maxAttempts))
		createDbInstanceReadReplicaOutput, err = w.rds.CreateDBInstanceReadReplica(ctx, createReadReplicaParams)
		if err != nil {
			var invalidDbInstanceStateErr *rdsTypes.InvalidDBInstanceStateFault
			if errors.As(err, &invalidDbInstanceStateErr) {
				attempts += 1
				time.Sleep(w.settings.PollAwsMinDelay)
				continue
			} else {
				return createDbInstanceReadReplicaOutput, err
			}
		}
		createDbInstanceReplicaSuccess = true
	}

	return createDbInstanceReadReplicaOutput, err
}

func (w *WorkerUtils) waitAndCreateDBReadReplica(
	ctx context.Context,
	operation base.Operation,
	i *RDSInstance,
	plan *catalog.RDSPlan,
) error {
	err := w.waitForDbReady(ctx, operation, i, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica, error waiting for database to be ready: %w", err)
	}

	jobs.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Creating database read replica")

	createReplicaOutput, err := w.createDBReadReplica(ctx, i, plan)
	if err != nil {
		w.logger.Error("waitAndCreateDBReadReplica: createDBReadReplica failed", "err", err)
		jobs.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Creating database read replica failed: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = w.waitForDbReady(ctx, operation, i, i.ReplicaDatabase)
	if err != nil {
		w.logger.Error("waitAndCreateDBReadReplica: waitForDbReady failed", "err", err)
		jobs.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for replica database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = w.updateDBTags(ctx, i, *createReplicaOutput.DBInstance.DBInstanceArn)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	return nil
}

func (w *WorkerUtils) waitForDbDeleted(ctx context.Context, operation base.Operation, i *RDSInstance, database string) error {
	w.logger.Debug(fmt.Sprintf("Waiting for DB instance %s to be deleted", database))

	// Create a waiter
	waiter := rds.NewDBInstanceDeletedWaiter(w.rds, func(dawo *rds.DBInstanceDeletedWaiterOptions) {
		dawo.MinDelay = w.settings.PollAwsMinDelay
	})

	// Define the waiting strategy
	maxWaitTime := getPollAwsMaxWaitTime(i.AllocatedStorage, w.settings.PollAwsMaxDuration)

	waiterInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	}
	err := waiter.Wait(ctx, waiterInput, maxWaitTime)

	if err != nil {
		updateErr := jobs.WriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed waiting for database to be deleted: %s", err))
		if updateErr != nil {
			err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
		}
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func (w *WorkerUtils) deleteDatabaseInstance(ctx context.Context, i *RDSInstance, operation base.Operation, database string) error {
	params := prepareDeleteDbInput(database)
	_, err := w.rds.DeleteDBInstance(ctx, params)
	if err != nil {
		if isDatabaseInstanceNotFoundError(err) {
			w.logger.Debug(fmt.Sprintf("database %s was already deleted, continuing", database))
			return nil
		} else {
			return fmt.Errorf("deleteDatabaseInstance: %w", err)
		}
	}

	err = w.waitForDbDeleted(ctx, operation, i, database)
	if err != nil {
		return fmt.Errorf("deleteDatabaseInstance: %w", err)
	}

	return nil
}

func (w *WorkerUtils) deleteDatabaseReadReplica(ctx context.Context, i *RDSInstance, operation base.Operation) error {
	err := w.deleteDatabaseInstance(ctx, i, operation, i.ReplicaDatabase)
	if err != nil {
		return fmt.Errorf("deleteDatabaseReadReplica: %w", err)
	}
	i.ReplicaDatabase = ""
	return nil
}
