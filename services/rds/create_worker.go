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
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/riverqueue/river"
)

const (
	CreateKind = "rds-create"
)

type CreateArgs struct {
	Instance *RDSInstance
	Plan     *catalog.RDSPlan
}

func (CreateArgs) Kind() string { return CreateKind }

type CreateWorker struct {
	river.WorkerDefaults[CreateArgs]
	settings             *config.Settings
	rds                  RDSClientInterface
	logger               *slog.Logger
	parameterGroupClient parameterGroupClient
	dbUtils              CredentialUtils
}

func NewCreateWorker(
	settings *config.Settings,
	rds RDSClientInterface,
	logger *slog.Logger,
	parameterGroupClient parameterGroupClient,
	dbUtils CredentialUtils,
) *CreateWorker {
	return &CreateWorker{
		settings:             settings,
		rds:                  rds,
		logger:               logger,
		parameterGroupClient: parameterGroupClient,
		dbUtils:              dbUtils,
	}
}

func (w *CreateWorker) Work(ctx context.Context, job *river.Job[CreateArgs]) error {
	err := w.asyncCreateDB(ctx, job.Args.Instance, job.Args.Plan)
	return err
}

func (w *CreateWorker) prepareCreateDbInput(
	i *RDSInstance,
	plan *catalog.RDSPlan,
	password string,
) (*rds.CreateDBInstanceInput, error) {
	rdsTags := ConvertTagsToRDSTags(i.getTags())

	allocatedStorage, err := common.ConvertInt64ToInt32Safely(i.AllocatedStorage)
	if err != nil {
		return nil, err
	}

	backupRetentionPeriod, err := common.ConvertInt64ToInt32Safely(i.BackupRetentionPeriod)
	if err != nil {
		return nil, err
	}

	// Standard parameters
	params := &rds.CreateDBInstanceInput{
		AllocatedStorage: allocatedStorage,
		// Instance class is defined by the plan
		DBInstanceClass:         &plan.InstanceClass,
		DBInstanceIdentifier:    &i.Database,
		DBName:                  aws.String(formatDBName(i.Database)),
		Engine:                  aws.String(i.DbType),
		MasterUserPassword:      &password,
		MasterUsername:          &i.Username,
		AutoMinorVersionUpgrade: aws.Bool(true),
		MultiAZ:                 aws.Bool(plan.Redundant),
		StorageEncrypted:        aws.Bool(plan.Encrypted),
		StorageType:             aws.String(i.StorageType),
		Tags:                    rdsTags,
		PubliclyAccessible:      aws.Bool(w.settings.PubliclyAccessibleFeature && i.PubliclyAccessible),
		BackupRetentionPeriod:   backupRetentionPeriod,
		DBSubnetGroupName:       &i.DbSubnetGroup,
		VpcSecurityGroupIds: []string{
			i.SecGroup,
		},
	}

	if i.DbVersion != "" {
		params.EngineVersion = aws.String(i.DbVersion)
	}
	if i.LicenseModel != "" {
		params.LicenseModel = aws.String(i.LicenseModel)
	}

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

func (w *CreateWorker) createDBReadReplica(ctx context.Context, i *RDSInstance, plan *catalog.RDSPlan) (*rds.CreateDBInstanceReadReplicaOutput, error) {
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

func (w *CreateWorker) waitAndCreateDBReadReplica(
	ctx context.Context,
	operation base.Operation,
	i *RDSInstance,
	plan *catalog.RDSPlan,
) error {
	db := getDbConnectionFromContext(ctx)

	err := waitForDbReady(ctx, w.rds, db, w.logger, w.settings, operation, i, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica, error waiting for database to be ready: %w", err)
	}

	jobs.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Creating database read replica")

	createReplicaOutput, err := w.createDBReadReplica(ctx, i, plan)
	if err != nil {
		w.logger.Error("waitAndCreateDBReadReplica: createDBReadReplica failed", "err", err)
		jobs.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Creating database read replica failed: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = waitForDbReady(ctx, w.rds, db, w.logger, w.settings, operation, i, i.ReplicaDatabase)
	if err != nil {
		w.logger.Error("waitAndCreateDBReadReplica: waitForDbReady failed", "err", err)
		jobs.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for replica database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = updateDBTags(ctx, w.rds, i, *createReplicaOutput.DBInstance.DBInstanceArn)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	return nil
}

func (w *CreateWorker) asyncCreateDB(ctx context.Context, i *RDSInstance, plan *catalog.RDSPlan) error {
	operation := base.CreateOp
	db := getDbConnectionFromContext(ctx)

	password, err := w.dbUtils.getPassword(i.Salt, i.Password, w.settings.EncryptionKey)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error getting password: %s", err))
		return river.JobCancel(fmt.Errorf("asyncCreateDB: error getting password %w ", err))
	}

	createDbInputParams, err := w.prepareCreateDbInput(i, plan, password)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error generating database creation params: %s", err))
		return river.JobCancel(fmt.Errorf("asyncCreateDB: prepareCreateDbInput error: %w ", err))
	}

	_, err = w.rds.CreateDBInstance(ctx, createDbInputParams)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error creating database: %s", err))
		return river.JobCancel(fmt.Errorf("asyncCreateDB: CreateDBInstance error: %w ", err))
	}

	err = waitForDbReady(ctx, w.rds, db, w.logger, w.settings, operation, i, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return river.JobCancel(fmt.Errorf("asyncCreateDB: waitForDbReady error: %w ", err))
	}

	if i.AddReadReplica {
		err := w.waitAndCreateDBReadReplica(ctx, operation, i, plan)
		if err != nil {
			jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error creating database replica: %s", err))
			return river.JobCancel(fmt.Errorf("asyncCreateDB: waitAndCreateDBReadReplica error: %w ", err))
		}
	}

	jobs.ShouldWriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished creating database resources")
	return nil
}
