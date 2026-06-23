package rds

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	DeleteKind = "rds-delete"
)

type DeleteArgs struct {
	Instance *RDSInstance `json:"instance"`
}

func (DeleteArgs) Kind() string { return DeleteKind }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	db                   *gorm.DB
	settings             *config.Settings
	rds                  RDSClientInterface
	logger               *slog.Logger
	parameterGroupClient parameterGroupClient
	optionGroupClient    optionGroupClient
	credentialUtils      CredentialUtils
}

func NewDeleteWorker(
	db *gorm.DB,
	settings *config.Settings,
	rds RDSClientInterface,
	logger *slog.Logger,
	parameterGroupClient parameterGroupClient,
	optionGroupClient optionGroupClient,
	credentialUtils CredentialUtils,
) *DeleteWorker {
	return &DeleteWorker{
		db:                   db,
		settings:             settings,
		rds:                  rds,
		logger:               logger,
		parameterGroupClient: parameterGroupClient,
		optionGroupClient:    optionGroupClient,
		credentialUtils:      credentialUtils,
	}
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return w.asyncDeleteDB(ctx, job.Args.Instance)
}

func (w *DeleteWorker) waitForDbDeleted(ctx context.Context, operation base.Operation, i *RDSInstance, database string) error {
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
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed waiting for database to be deleted: %s", err))
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func (w *DeleteWorker) deleteDatabaseInstance(ctx context.Context, i *RDSInstance, operation base.Operation, database string) error {
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

func (w *DeleteWorker) deleteDatabaseReadReplica(ctx context.Context, i *RDSInstance, operation base.Operation) error {
	err := w.deleteDatabaseInstance(ctx, i, operation, i.ReplicaDatabase)
	if err != nil {
		return fmt.Errorf("deleteDatabaseReadReplica: %w", err)
	}
	i.ReplicaDatabase = ""
	return nil
}

func (w *DeleteWorker) asyncDeleteDB(ctx context.Context, i *RDSInstance) error {
	operation := base.DeleteOp

	if i.ReplicaDatabase != "" {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")
		err := w.deleteDatabaseReadReplica(ctx, i, operation)
		if err != nil {
			asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete replica database: %s", err))
			w.logger.Error("asyncDeleteDB: deleteDatabaseReadReplica error", "err", err)
			return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting replica %w ", err))
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database")
	err := w.deleteDatabaseInstance(ctx, i, operation, i.Database)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete database: %s", err))
		w.logger.Error("asyncDeleteDB: deleteDatabaseInstance error", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting database %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting parameter group")
	err = w.parameterGroupClient.DeleteParameterGroup(i.ParameterGroupName)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete parameter group: %s", err))
		w.logger.Error("asyncDeleteDB: DeleteParameterGroup error", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting parameter group %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up parameter groups")
	err = w.parameterGroupClient.CleanupCustomParameterGroups()
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to cleanup parameter groups: %s", err))
		w.logger.Error("asyncDeleteDB: CleanupCustomParameterGroups error", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting parameter groups %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting option group")
	err = w.optionGroupClient.DeleteOptionGroup(i.OptionGroupName)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete option group: %s", err))
		w.logger.Error("asyncDeleteDB: DeleteOptionGroup error", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting option group %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up option groups")
	err = w.optionGroupClient.CleanupCustomOptionGroups()
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to cleanup option groups: %s", err))
		w.logger.Error("asyncDeleteDB: CleanupCustomOptionGroups error", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting option groups %w ", err))
	}

	err = w.db.Unscoped().Delete(i).Error
	if err != nil {
		w.logger.Error("asyncDeleteDB: error deleting record", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting record %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted database resources")
	return nil
}
