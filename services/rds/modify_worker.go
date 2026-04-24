package rds

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	ModifyKind = "rds-modify"
)

type ModifyArgs struct {
	Instance *RDSInstance     `json:"instance"`
	Plan     *catalog.RDSPlan `json:"plan"`
}

func (ModifyArgs) Kind() string { return ModifyKind }

type ModifyWorker struct {
	river.WorkerDefaults[ModifyArgs]
	db                   *gorm.DB
	settings             *config.Settings
	rds                  RDSClientInterface
	logger               *slog.Logger
	parameterGroupClient parameterGroupClient
	credentialUtils      CredentialUtils
}

func NewModifyWorker(
	db *gorm.DB,
	settings *config.Settings,
	rds RDSClientInterface,
	logger *slog.Logger,
	parameterGroupClient parameterGroupClient,
	credentialUtils CredentialUtils,
) *ModifyWorker {
	return &ModifyWorker{
		db:                   db,
		settings:             settings,
		rds:                  rds,
		logger:               logger,
		parameterGroupClient: parameterGroupClient,
		credentialUtils:      credentialUtils,
	}
}

func (w *ModifyWorker) Work(ctx context.Context, job *river.Job[ModifyArgs]) error {
	return w.asyncModifyDb(ctx, job.Args.Instance, job.Args.Plan)
}

func (w *ModifyWorker) prepareModifyDbInstanceInput(
	i *RDSInstance,
	plan *catalog.RDSPlan,
	database string,
	isReplica bool,
) (*rds.ModifyDBInstanceInput, error) {
	// Standard parameters (https://docs.aws.amazon.com/sdk-for-go/api/service/rds/#RDS.ModifyDBInstance)
	// These actions are applied immediately.
	allocatedStorage, err := common.ConvertInt64ToInt32Safely(i.AllocatedStorage)
	if err != nil {
		return nil, err
	}

	backupRetentionPeriod, err := common.ConvertInt64ToInt32Safely(i.BackupRetentionPeriod)
	if err != nil {
		return nil, err
	}

	params := &rds.ModifyDBInstanceInput{
		AllocatedStorage:         allocatedStorage,
		ApplyImmediately:         aws.Bool(true),
		DBInstanceClass:          &plan.InstanceClass,
		MultiAZ:                  &plan.Redundant,
		DBInstanceIdentifier:     &database,
		AllowMajorVersionUpgrade: aws.Bool(i.AllowMajorVersionUpgrade),
		BackupRetentionPeriod:    backupRetentionPeriod,
	}

	if i.DbVersion != "" {
		params.EngineVersion = aws.String(i.DbVersion)
	}

	if i.StorageType != "" {
		params.StorageType = aws.String(i.StorageType)
	}

	if i.RotateCredentials && !isReplica {
		// if i.credentialUtils == nil {
		// 	i.credentialUtils = &RDSCredentialUtils{}
		// }
		password, err := i.credentialUtils.getPassword(i.Salt, i.Password, w.settings.EncryptionKey)
		if err != nil {
			return nil, err
		}
		params.MasterUserPassword = aws.String(password)
	}

	rdsTags := ConvertTagsToRDSTags(i.getTags())

	// If a custom parameter has been requested, and the feature is enabled,
	// create/update a custom parameter group for our custom parameters.
	err = w.parameterGroupClient.ProvisionCustomParameterGroupIfNecessary(i, rdsTags)
	if err != nil {
		return nil, err
	}
	if i.ParameterGroupName != "" {
		params.DBParameterGroupName = aws.String(i.ParameterGroupName)
	}
	return params, nil
}

func (w *ModifyWorker) asyncModifyDbInstance(ctx context.Context, operation base.Operation, i *RDSInstance, plan *catalog.RDSPlan, database string, isReplica bool) error {
	modifyParams, err := w.prepareModifyDbInstanceInput(i, plan, database, isReplica)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error preparing database modify parameters: %s", err))
		return fmt.Errorf("asyncModifyDb, error preparing modify database input: %w", err)
	}

	err = waitForDbReady(ctx, w.db, w.settings, w.rds, w.logger, operation, i, database)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("asyncModifyDbInstance, error waiting for database to be ready: %w", err)
	}

	modifyOutput, err := w.rds.ModifyDBInstance(ctx, modifyParams)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database: %s", err))
		return fmt.Errorf("asyncModifyDb, error modifying database instance: %w", err)
	}

	err = waitForDbReady(ctx, w.db, w.settings, w.rds, w.logger, operation, i, database)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("asyncModifyDbInstance, error waiting for database to be ready: %w", err)
	}

	err = updateDBTags(ctx, w.rds, i, *modifyOutput.DBInstance.DBInstanceArn)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("asyncModifyDb, error updating replica tags: %w", err)
	}

	return nil
}

func (w *ModifyWorker) asyncModifyDb(ctx context.Context, i *RDSInstance, plan *catalog.RDSPlan) error {
	operation := base.ModifyOp

	err := w.asyncModifyDbInstance(ctx, operation, i, plan, i.Database, false)
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database: %s", err))
		w.logger.Error("asyncModifyDb: asyncModifyDbInstance error", "err", err)
		return river.JobCancel(fmt.Errorf("asyncModifyDb: error modifying database instance %w ", err))
	}

	if i.AddReadReplica {
		// Add new read replica
		err = waitAndCreateDBReadReplica(ctx, w.db, w.settings, w.rds, w.logger, operation, i, plan)
		if err != nil {
			asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error creating database replica: %s", err))
			w.logger.Error("asyncModifyDb: waitAndCreateDBReadReplica error", "err", err)
			return river.JobCancel(fmt.Errorf("asyncModifyDb: error creating database replica %w ", err))
		}
	} else if !i.DeleteReadReplica && !i.AddReadReplica && i.ReplicaDatabase != "" {
		err := w.asyncModifyDbInstance(ctx, operation, i, plan, i.ReplicaDatabase, true)
		if err != nil {
			asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database replica: %s", err))
			w.logger.Error("asyncModifyDb: asyncModifyDbInstance read replica error", "err", err)
			return river.JobCancel(fmt.Errorf("asyncModifyDb: error modifying database replica %w ", err))
		}
	}

	if i.DeleteReadReplica {
		err = deleteDatabaseReadReplica(ctx, w.db, w.settings, w.rds, w.logger, i, operation)
		if err != nil {
			asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error deleting database replica: %s", err))
			w.logger.Error("asyncModifyDb: deleteDatabaseReadReplica error", "err", err)
			return river.JobCancel(fmt.Errorf("asyncModifyDb: error deleting database replica %w ", err))
		}
	}

	err = w.db.Save(i).Error
	if err != nil {
		asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error saving record: %s", err))
		w.logger.Error("asyncModifyDb: error saving record", "err", err)
		return river.JobCancel(fmt.Errorf("asyncModifyDb: error saving database record %w ", err))
	}

	asyncmessage.ShouldWriteAsyncJobMessage(w.db, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished modifying database resources")
	return nil
}
