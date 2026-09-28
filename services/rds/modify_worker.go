package rds

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
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

var (
	ErrDeletingDBOptionGroup    = errors.New("deleting database option group")
	ErrDeletingDBParameterGroup = errors.New("deleting database parameter group")
	ErrDeletingDBReplica        = errors.New("deleting database replica")
	ErrModifyingDBReplica       = errors.New("modifying database replica")
	ErrModifyingDB              = errors.New("modifying database")
	ErrSavingInstance           = errors.New("saving updated instance")
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
	optionGroupClient    optionGroupClient
	credentialUtils      CredentialUtils
}

func NewModifyWorker(
	db *gorm.DB,
	settings *config.Settings,
	rds RDSClientInterface,
	logger *slog.Logger,
	parameterGroupClient parameterGroupClient,
	optionGroupClient optionGroupClient,
	credentialUtils CredentialUtils,
) *ModifyWorker {
	return &ModifyWorker{
		db:                   db,
		settings:             settings,
		rds:                  rds,
		logger:               logger,
		parameterGroupClient: parameterGroupClient,
		optionGroupClient:    optionGroupClient,
		credentialUtils:      credentialUtils,
	}
}

func (w *ModifyWorker) Work(ctx context.Context, job *river.Job[ModifyArgs]) error {
	operation := base.ModifyOp
	i := job.Args.Instance
	plan := job.Args.Plan
	errChan := make(chan error, 1)

	go func(ctx context.Context, i *RDSInstance, operation base.Operation, plan *catalog.RDSPlan) {
		errChan <- w.asyncModifyDb(ctx, operation, i, plan)
	}(ctx, i, operation, plan)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		if err != nil {
			w.logger.Error("error modifying database", "err", err)
			asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, err.Error())
			return river.JobCancel(err)
		}
		return nil
	}
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

	rdsTags := ConvertTagsToRDSTags(i.getTags())

	// If a custom parameter has been requested, and the feature is enabled,
	// create/update a custom parameter group for our custom parameters.
	_, err = w.parameterGroupClient.ProvisionOrModifyCustomParameterGroup(i, rdsTags)
	if err != nil {
		return nil, err
	}
	if i.ParameterGroupName != "" {
		params.DBParameterGroupName = &i.ParameterGroupName
	}

	// if the instance has a custom option group and there is a major version upgrade
	// rebuild an equivalent group for new major version
	_, err = w.optionGroupClient.ProvisionOrModifyCustomOptionGroup(i, rdsTags)
	if err != nil {
		return nil, err
	}
	if i.OptionGroupName != "" {
		params.OptionGroupName = &i.OptionGroupName
	}

	if i.DbVersion != "" {
		params.EngineVersion = aws.String(i.DbVersion)
	}

	if i.StorageType != "" {
		params.StorageType = aws.String(i.StorageType)
	}

	if i.RotateCredentials && !isReplica {
		password, err := w.credentialUtils.decryptCredential(i.Salt, i.Password, w.settings.EncryptionKey)
		if err != nil {
			return nil, err
		}
		params.MasterUserPassword = aws.String(password)
	}

	if len(i.EnabledCloudwatchLogGroupExports) > 0 {
		params.CloudwatchLogsExportConfiguration = &rdsTypes.CloudwatchLogsExportConfiguration{
			EnableLogTypes: i.EnabledCloudwatchLogGroupExports,
		}
	}
	return params, nil
}

func (w *ModifyWorker) asyncModifyDbInstance(ctx context.Context, operation base.Operation, i *RDSInstance, plan *catalog.RDSPlan, database string, isReplica bool) error {
	databaseOperationTarget := "primary database"
	if isReplica {
		databaseOperationTarget = "replica database"
	}

	modifyParams, err := w.prepareModifyDbInstanceInput(i, plan, database, isReplica)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error preparing database modify parameters: %s", err))
		return fmt.Errorf("asyncModifyDb, error preparing modify database input: %w", err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Waiting for %s to be ready", databaseOperationTarget))
	err = waitForDbReady(ctx, w.db, w.settings, w.rds, w.logger, operation, i, database)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("asyncModifyDbInstance, error waiting for database to be ready: %w", err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Modifying %s", databaseOperationTarget))
	modifyOutput, err := w.rds.ModifyDBInstance(ctx, modifyParams)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database: %s", err))
		return fmt.Errorf("asyncModifyDb, error modifying database instance: %w", err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Waiting for %s to be ready", databaseOperationTarget))
	err = waitForDbReady(ctx, w.db, w.settings, w.rds, w.logger, operation, i, database)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("asyncModifyDbInstance, error waiting for database to be ready: %w", err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Updating %s tags", databaseOperationTarget))
	err = updateDBTags(ctx, w.rds, i, *modifyOutput.DBInstance.DBInstanceArn)
	if err != nil {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("asyncModifyDb, error updating replica tags: %w", err)
	}

	return nil
}

func (w *ModifyWorker) asyncModifyDb(ctx context.Context, operation base.Operation, i *RDSInstance, plan *catalog.RDSPlan) error {
	serviceID := i.ServiceID
	uuid := i.Uuid

	existingParameterGroupName := i.ParameterGroupName
	existingOptionGroupName := i.OptionGroupName

	if i.AddReadReplica {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Creating database replica")
		// Add new read replica
		err := waitAndCreateDBReadReplica(ctx, w.db, w.settings, w.rds, w.logger, operation, i, plan)
		if err != nil {
			return common.FmtErr(ErrCreatingDBReplica, err)
		}
	} else if !i.DeleteReadReplica && !i.AddReadReplica && i.ReplicaDatabase != "" {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Modifying database replica")
		err := w.asyncModifyDbInstance(ctx, operation, i, plan, i.ReplicaDatabase, true)
		if err != nil {
			return common.FmtErr(ErrModifyingDBReplica, err)
		}
	}

	if i.DeleteReadReplica {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")
		err := deleteDatabaseReadReplica(ctx, w.db, w.settings, w.rds, w.logger, i, operation)
		if err != nil {
			return common.FmtErr(ErrDeletingDBReplica, err)
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Modifying database instance")
	err := w.asyncModifyDbInstance(ctx, operation, i, plan, i.Database, false)
	if err != nil {
		return common.FmtErr(ErrModifyingDB, err)
	}

	if existingParameterGroupName != "" && i.ParameterGroupName != existingParameterGroupName {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting old parameter group")
		err = w.parameterGroupClient.DeleteParameterGroup(existingParameterGroupName)
		if err != nil {
			return common.FmtErr(ErrDeletingDBParameterGroup, err)
		}
	}

	if existingOptionGroupName != "" && i.OptionGroupName != existingOptionGroupName {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting old option group")
		// best effort deletion. Option group might still be attached to snapshots (preventing deletion), so leave it for later cleanup
		err = w.optionGroupClient.DeleteOptionGroup(existingOptionGroupName)
		if err != nil {
			return common.FmtErr(ErrDeletingDBOptionGroup, err)
		}
	}

	err = w.db.Save(i).Error
	if err != nil {
		return common.FmtErr(ErrSavingInstance, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, serviceID, uuid, operation, base.InstanceReady, "Finished modifying database resources")
	return nil
}
