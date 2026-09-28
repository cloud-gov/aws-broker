package rds

import (
	"context"
	"errors"
	"log/slog"

	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

var (
	ErrDeletingDB                = errors.New("deleting database")
	ErrDeletingDBInstance        = errors.New("deleting database instance")
	ErrDeletingDBOptionGroups    = errors.New("deleting databse option groups")
	ErrDeletingDBParameterGroups = errors.New("deleting database parameter groups")
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
	operation := base.DeleteOp
	i := job.Args.Instance
	errChan := make(chan error, 1)

	go func(ctx context.Context, i *RDSInstance, operation base.Operation) {
		errChan <- w.asyncDeleteDB(ctx, i, operation)
	}(ctx, i, operation)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		if err != nil {
			w.logger.Error("error deleting database", "err", err)
			asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, err.Error())
			return river.JobCancel(err)
		}
		return nil
	}
}

func (w *DeleteWorker) asyncDeleteDB(ctx context.Context, i *RDSInstance, operation base.Operation) error {
	if i.ReplicaDatabase != "" {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")
		err := deleteDatabaseReadReplica(ctx, w.db, w.settings, w.rds, w.logger, i, operation)
		if err != nil {
			return common.FmtErr(ErrDeletingDBReplica, err)
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database")
	err := deleteDatabaseInstance(ctx, w.db, w.settings, w.rds, w.logger, i, operation, i.Database)
	if err != nil {
		return common.FmtErr(ErrDeletingDB, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting parameter group")
	err = w.parameterGroupClient.DeleteParameterGroup(i.ParameterGroupName)
	if err != nil {
		return common.FmtErr(ErrDeletingDBParameterGroup, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up parameter groups")
	err = w.parameterGroupClient.CleanupCustomParameterGroups()
	if err != nil {
		return common.FmtErr(ErrDeletingDBParameterGroups, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting option group")
	err = w.optionGroupClient.DeleteOptionGroup(i.OptionGroupName)
	if err != nil {
		return common.FmtErr(ErrDeletingDBOptionGroup, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up option groups")
	err = w.optionGroupClient.CleanupCustomOptionGroups()
	if err != nil {
		return common.FmtErr(ErrDeletingDBOptionGroups, err)
	}

	err = w.db.Unscoped().Delete(i).Error
	if err != nil {
		return common.FmtErr(ErrDeletingDBInstance, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted database resources")
	return nil
}
