package rds

import (
	"context"
	"fmt"
	"log/slog"

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

func (w *DeleteWorker) asyncDeleteDB(ctx context.Context, i *RDSInstance) error {
	operation := base.DeleteOp

	if i.ReplicaDatabase != "" {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")
		err := deleteDatabaseReadReplica(ctx, w.db, w.settings, w.rds, w.logger, i, operation)
		if err != nil {
			asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete replica database: %s", err))
			w.logger.Error("asyncDeleteDB: deleteDatabaseReadReplica error", "err", err)
			return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting replica %w ", err))
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database")
	err := deleteDatabaseInstance(ctx, w.db, w.settings, w.rds, w.logger, i, operation, i.Database)
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
		// best effort deletion. Option group might still be attached to snapshots (preventing deletion), so leave it for later cleanup
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "asyncModifyDbInstance: deletion of old option group failed; leaving for later cleanup")
		w.logger.Warn("asyncDeleteDb: deletion of option group failed; leaving for later cleanup", "err", err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up option groups")
	err = w.optionGroupClient.CleanupCustomOptionGroups()
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Failed to cleanup option groups: %s", err))
		w.logger.Warn("asyncDeleteDB: CleanupCustomOptionGroups error", "err", err)
	}

	err = w.db.Unscoped().Delete(i).Error
	if err != nil {
		w.logger.Error("asyncDeleteDB: error deleting record", "err", err)
		return river.JobCancel(fmt.Errorf("asyncDeleteDB: error deleting record %w ", err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted database resources")
	return nil
}
