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
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"gorm.io/gorm"
)

func waitForDbReady(
	ctx context.Context,
	db *gorm.DB,
	settings *config.Settings,
	rdsClient RDSClientInterface,
	logger *slog.Logger,
	operation base.Operation,
	i *RDSInstance,
	database string,
) error {
	logger.Debug(fmt.Sprintf("Waiting for DB instance %s to be available", database))

	// Create a waiter
	waiter := rds.NewDBInstanceAvailableWaiter(rdsClient, func(dawo *rds.DBInstanceAvailableWaiterOptions) {
		dawo.MinDelay = settings.PollAwsMinDelay
		dawo.LogWaitAttempts = true
	})

	// Define the waiting strategy
	maxWaitTime := getPollAwsMaxWaitTime(i.AllocatedStorage, settings.PollAwsMaxDuration)

	waiterInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	}
	err := waiter.Wait(ctx, waiterInput, maxWaitTime)

	if err != nil {
		updateErr := asyncmessage.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Failed waiting for database to become available: %s", err))
		if updateErr != nil {
			err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
		}
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func updateDBTags(ctx context.Context, rdsClient RDSClientInterface, i *RDSInstance, dbInstanceARN string) error {
	_, err := rdsClient.AddTagsToResource(ctx, &rds.AddTagsToResourceInput{
		ResourceName: aws.String(dbInstanceARN),
		Tags:         ConvertTagsToRDSTags(i.getTags()),
	})
	return err
}

func createDBReadReplica(
	ctx context.Context,
	settings *config.Settings,
	rdsClient RDSClientInterface,
	logger *slog.Logger,
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
		PubliclyAccessible:         aws.Bool(settings.PubliclyAccessibleFeature && i.PubliclyAccessible),
		StorageType:                aws.String(i.StorageType),
		Tags:                       rdsTags,
		VpcSecurityGroupIds: []string{
			i.SecGroup,
		},
	}

	var createDbInstanceReplicaSuccess bool
	var createDbInstanceReadReplicaOutput *rds.CreateDBInstanceReadReplicaOutput

	attempts := 1
	maxRetries := getPollAwsMaxRetries(i.AllocatedStorage, settings.PollAwsMaxRetries)
	// max attempts = initial attempt + retries
	maxAttempts := 1 + maxRetries

	for !createDbInstanceReplicaSuccess && attempts <= maxAttempts {
		logger.Info(fmt.Sprintf("attempting replica creation. attempt %d of %d", attempts, maxAttempts))
		createDbInstanceReadReplicaOutput, err = rdsClient.CreateDBInstanceReadReplica(ctx, createReadReplicaParams)
		if err != nil {
			var invalidDbInstanceStateErr *rdsTypes.InvalidDBInstanceStateFault
			if errors.As(err, &invalidDbInstanceStateErr) {
				attempts += 1
				time.Sleep(settings.PollAwsMinDelay)
				continue
			} else {
				return createDbInstanceReadReplicaOutput, err
			}
		}
		createDbInstanceReplicaSuccess = true
	}

	return createDbInstanceReadReplicaOutput, err
}

func waitAndCreateDBReadReplica(
	ctx context.Context,
	db *gorm.DB,
	settings *config.Settings,
	rdsClient RDSClientInterface,
	logger *slog.Logger,
	operation base.Operation,
	i *RDSInstance,
	plan *catalog.RDSPlan,
) error {
	err := waitForDbReady(ctx, db, settings, rdsClient, logger, operation, i, i.Database)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica, error waiting for database to be ready: %w", err)
	}

	asyncmessage.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Creating database read replica")

	createReplicaOutput, err := createDBReadReplica(ctx, settings, rdsClient, logger, i, plan)
	if err != nil {
		logger.Error("waitAndCreateDBReadReplica: createDBReadReplica failed", "err", err)
		asyncmessage.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Creating database read replica failed: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = waitForDbReady(ctx, db, settings, rdsClient, logger, operation, i, i.ReplicaDatabase)
	if err != nil {
		logger.Error("waitAndCreateDBReadReplica: waitForDbReady failed", "err", err)
		asyncmessage.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for replica database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = updateDBTags(ctx, rdsClient, i, *createReplicaOutput.DBInstance.DBInstanceArn)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	return nil
}

func waitForDbDeleted(
	ctx context.Context,
	db *gorm.DB,
	settings *config.Settings,
	rdsClient RDSClientInterface,
	logger *slog.Logger,
	operation base.Operation,
	i *RDSInstance,
	database string,
) error {
	logger.Debug(fmt.Sprintf("Waiting for DB instance %s to be deleted", database))

	// Create a waiter
	waiter := rds.NewDBInstanceDeletedWaiter(rdsClient, func(dawo *rds.DBInstanceDeletedWaiterOptions) {
		dawo.MinDelay = settings.PollAwsMinDelay
	})

	// Define the waiting strategy
	maxWaitTime := getPollAwsMaxWaitTime(i.AllocatedStorage, settings.PollAwsMaxDuration)

	waiterInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	}
	err := waiter.Wait(ctx, waiterInput, maxWaitTime)

	if err != nil {
		updateErr := asyncmessage.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed waiting for database to be deleted: %s", err))
		if updateErr != nil {
			err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
		}
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func deleteDatabaseInstance(
	ctx context.Context,
	db *gorm.DB,
	settings *config.Settings,
	rdsClient RDSClientInterface,
	logger *slog.Logger,
	i *RDSInstance,
	operation base.Operation,
	database string,
) error {
	params := prepareDeleteDbInput(database)
	_, err := rdsClient.DeleteDBInstance(ctx, params)
	if err != nil {
		if isDatabaseInstanceNotFoundError(err) {
			logger.Debug(fmt.Sprintf("database %s was already deleted, continuing", database))
			return nil
		} else {
			return fmt.Errorf("deleteDatabaseInstance: %w", err)
		}
	}

	err = waitForDbDeleted(ctx, db, settings, rdsClient, logger, operation, i, database)
	if err != nil {
		return fmt.Errorf("deleteDatabaseInstance: %w", err)
	}

	return nil
}

func deleteDatabaseReadReplica(
	ctx context.Context,
	db *gorm.DB,
	settings *config.Settings,
	rdsClient RDSClientInterface,
	logger *slog.Logger,
	i *RDSInstance,
	operation base.Operation,
) error {
	err := deleteDatabaseInstance(ctx, db, settings, rdsClient, logger, i, operation, i.ReplicaDatabase)
	if err != nil {
		return fmt.Errorf("deleteDatabaseReadReplica: %w", err)
	}
	i.ReplicaDatabase = ""
	return nil
}

func prepareDeleteDbInput(database string) *rds.DeleteDBInstanceInput {
	return &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   aws.String(database), // Required
		DeleteAutomatedBackups: aws.Bool(false),
		SkipFinalSnapshot:      aws.Bool(true),
	}
}

func isDatabaseInstanceNotFoundError(err error) bool {
	var notFoundException *rdsTypes.DBInstanceNotFoundFault
	return errors.As(err, &notFoundException)
}
