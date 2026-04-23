package rds

import (
	"context"
	"database/sql"
	"log/slog"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/riverqueue/river"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/cloud-gov/aws-broker/base"
	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	jobs "github.com/cloud-gov/aws-broker/jobs"

	"errors"
	"fmt"
)

type dbAdapter interface {
	createDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error)
	modifyDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error)
	checkDBStatus(database string) (base.InstanceState, error)
	bindDBToApp(i *RDSInstance, password string) (map[string]string, error)
	deleteDB(i *RDSInstance) (base.InstanceState, error)
	describeDatabaseInstance(database string) (*rdsTypes.DBInstance, error)
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(
	ctx context.Context,
	s *config.Settings,
	db *gorm.DB,
	logger *slog.Logger,
	riverClient *river.Client[*sql.Tx],
) (dbAdapter, error) {
	// For test environments, use a mock broker.dbAdapter.
	if s.Environment == "test" {
		return &mockDBAdapter{
			db: db,
		}, nil
	}

	cfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithRegion(s.Region),
	)
	if err != nil {
		return nil, err
	}

	rdsClient := rds.NewFromConfig(cfg)

	parameterGroupClient := NewAwsParameterGroupClient(ctx, rdsClient, s)

	dbAdapter := NewRdsDedicatedDBAdapter(ctx, s, db, rdsClient, parameterGroupClient, logger, riverClient)
	return dbAdapter, nil
}

func NewRdsDedicatedDBAdapter(
	ctx context.Context,
	s *config.Settings,
	db *gorm.DB,
	rdsClient RDSClientInterface,
	parameterGroupClient parameterGroupClient,
	logger *slog.Logger,
	riverClient *river.Client[*sql.Tx],
) *dedicatedDBAdapter {
	return &dedicatedDBAdapter{
		ctx:                  ctx,
		settings:             *s,
		rds:                  rdsClient,
		parameterGroupClient: parameterGroupClient,
		db:                   db,
		logger:               logger,
		riverClient:          riverClient,
	}
}

// MockDBAdapter is a struct meant for testing.
// It should only be used in *_test.go files.
// It is only here because *_test.go files are only compiled during "go test"
// and it's referenced in non *_test.go code eg. InitializeAdapter in main.go.
type mockDBAdapter struct {
	db            *gorm.DB
	createDBState *base.InstanceState
}

func (d *mockDBAdapter) createDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error) {
	if d.createDBState != nil {
		return *d.createDBState, nil
	}
	return base.InstanceInProgress, nil
}

func (d *mockDBAdapter) modifyDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error) {
	err := d.db.Save(i).Error
	return base.InstanceInProgress, err
}

func (d *mockDBAdapter) checkDBStatus(database string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockDBAdapter) bindDBToApp(i *RDSInstance, password string) (map[string]string, error) {
	// TODO
	return i.getCredentials(password)
}

func (d *mockDBAdapter) deleteDB(i *RDSInstance) (base.InstanceState, error) {
	// TODO
	return base.InstanceInProgress, nil
}

func (d *mockDBAdapter) describeDatabaseInstance(database string) (*rdsTypes.DBInstance, error) {
	return nil, nil
}

// END MockDBAdpater
type DBEndpointDetails struct {
	Port  int64
	Host  string
	State base.InstanceState
}

type dedicatedDBAdapter struct {
	ctx                  context.Context
	settings             config.Settings
	rds                  RDSClientInterface
	parameterGroupClient parameterGroupClient
	db                   *gorm.DB
	logger               *slog.Logger
	riverClient          *river.Client[*sql.Tx]
}

func (d *dedicatedDBAdapter) createDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error) {
	err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.CreateOp, base.InstanceInProgress, "Database creation in progress")
	if err != nil {
		return base.InstanceNotCreated, err
	}

	tx := d.db.Begin()
	if err := tx.Error; err != nil {
		return base.InstanceNotCreated, err
	}
	defer tx.Rollback()

	sqlTx := tx.Statement.ConnPool.(*sql.Tx)

	_, err = d.riverClient.InsertTx(d.ctx, sqlTx, &CreateArgs{
		Instance: i,
		Plan:     plan,
	}, nil)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	if err := tx.Commit().Error; err != nil {
		return base.InstanceNotCreated, err
	}

	return base.InstanceInProgress, nil
}

// This should ultimately get exposed as part of the "update-service" method for the broker:
// cf update-service SERVICE_INSTANCE [-p NEW_PLAN] [-c PARAMETERS_AS_JSON] [-t TAGS] [--upgrade]
func (d *dedicatedDBAdapter) modifyDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error) {
	err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.ModifyOp, base.InstanceInProgress, "Database modification in progress")
	if err != nil {
		return base.InstanceNotModified, err
	}

	tx := d.db.Begin()
	if err := tx.Error; err != nil {
		return base.InstanceNotModified, err
	}
	defer tx.Rollback()

	sqlTx := tx.Statement.ConnPool.(*sql.Tx)

	_, err = d.riverClient.InsertTx(d.ctx, sqlTx, &ModifyArgs{
		Instance: i,
		Plan:     plan,
	}, nil)
	if err != nil {
		return base.InstanceNotModified, err
	}

	if err := tx.Commit().Error; err != nil {
		return base.InstanceNotModified, err
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedDBAdapter) describeDatabaseInstance(database string) (*rdsTypes.DBInstance, error) {
	params := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(database),
	}

	resp, err := d.rds.DescribeDBInstances(d.ctx, params)
	if err != nil {
		return nil, err
	}

	numOfInstances := len(resp.DBInstances)
	if numOfInstances == 0 {
		return nil, errors.New("could not find any instances")
	}

	if numOfInstances > 1 {
		return nil, fmt.Errorf("found more than one database for %s", database)
	}

	return &resp.DBInstances[0], nil
}

func (d *dedicatedDBAdapter) checkDBStatus(database string) (base.InstanceState, error) {
	dbInstance, err := d.describeDatabaseInstance(database)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	// Possible instance statuses: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html#Overview.DBInstance.Status
	switch *(dbInstance.DBInstanceStatus) {
	case "available":
		return base.InstanceReady, nil
	case "creating":
		return base.InstanceInProgress, nil
	case "deleting":
		return base.InstanceNotGone, nil
	case "failed":
		return base.InstanceNotCreated, nil
	default:
		return base.InstanceInProgress, nil
	}
}

func (d *dedicatedDBAdapter) getDatabaseEndpointProperties(database string) (*DBEndpointDetails, error) {
	dbInstance, err := d.describeDatabaseInstance(database)
	if err != nil {
		return nil, err
	}

	if dbInstance.DBInstanceStatus == nil || (dbInstance.DBInstanceStatus != nil && *(dbInstance.DBInstanceStatus) != "available") {
		return nil, errors.New("instance not available yet. Please wait and try again")
	}

	if dbInstance.Endpoint == nil || dbInstance.Endpoint.Address == nil || dbInstance.Endpoint.Port == nil {
		// Something went horribly wrong. Should never get here.
		return nil, errors.New("endpoint information not available for database")
	}

	return &DBEndpointDetails{
		Port:  int64(*dbInstance.Endpoint.Port),
		Host:  *(dbInstance.Endpoint.Address),
		State: base.InstanceReady,
	}, nil
}

func (d *dedicatedDBAdapter) bindDBToApp(i *RDSInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		dbEndpointDetails, err := d.getDatabaseEndpointProperties(i.Database)
		if err != nil {
			return nil, err
		}

		i.Port = dbEndpointDetails.Port
		i.Host = dbEndpointDetails.Host
		i.State = dbEndpointDetails.State
	}

	// handle replica creds
	if i.ReplicaDatabase != "" {
		dbEndpointDetails, err := d.getDatabaseEndpointProperties(i.ReplicaDatabase)
		if err != nil {
			return nil, err
		}

		i.ReplicaDatabaseHost = dbEndpointDetails.Host
	}

	err := d.db.Save(i).Error
	if err != nil {
		return nil, err
	}

	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(password)
}

func (d *dedicatedDBAdapter) waitForDbDeleted(operation base.Operation, i *RDSInstance, database string) error {
	d.logger.Debug(fmt.Sprintf("Waiting for DB instance %s to be deleted", database))

	// Create a waiter
	waiter := rds.NewDBInstanceDeletedWaiter(d.rds, func(dawo *rds.DBInstanceDeletedWaiterOptions) {
		dawo.MinDelay = d.settings.PollAwsMinDelay
	})

	// Define the waiting strategy
	maxWaitTime := getPollAwsMaxWaitTime(i.AllocatedStorage, d.settings.PollAwsMaxDuration)

	waiterInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	}
	err := waiter.Wait(d.ctx, waiterInput, maxWaitTime)

	if err != nil {
		updateErr := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed waiting for database to be deleted: %s", err))
		if updateErr != nil {
			err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
		}
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func (d *dedicatedDBAdapter) deleteDatabaseInstance(i *RDSInstance, operation base.Operation, database string) error {
	params := prepareDeleteDbInput(database)
	_, err := d.rds.DeleteDBInstance(d.ctx, params)
	if err != nil {
		if isDatabaseInstanceNotFoundError(err) {
			d.logger.Debug(fmt.Sprintf("database %s was already deleted, continuing", database))
			return nil
		} else {
			return fmt.Errorf("deleteDatabaseInstance: %w", err)
		}
	}

	err = d.waitForDbDeleted(operation, i, database)
	if err != nil {
		return fmt.Errorf("deleteDatabaseInstance: %w", err)
	}

	return nil
}

func (d *dedicatedDBAdapter) deleteDatabaseReadReplica(i *RDSInstance, operation base.Operation) error {
	err := d.deleteDatabaseInstance(i, operation, i.ReplicaDatabase)
	if err != nil {
		return fmt.Errorf("deleteDatabaseReadReplica: %w", err)
	}
	i.ReplicaDatabase = ""
	return nil
}

func (d *dedicatedDBAdapter) asyncDeleteDB(i *RDSInstance) {
	operation := base.DeleteOp

	if i.ReplicaDatabase != "" {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")
		err := d.deleteDatabaseReadReplica(i, operation)
		if err != nil {
			jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete replica database: %s", err))
			d.logger.Error("asyncDeleteDB: deleteDatabaseReadReplica error", "err", err)
			return
		}
	}

	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database")
	err := d.deleteDatabaseInstance(i, operation, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete database: %s", err))
		d.logger.Error("asyncDeleteDB: deleteDatabaseInstance error", "err", err)
		return
	}

	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up parameter groups")
	err = d.parameterGroupClient.CleanupCustomParameterGroups()
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to cleanup parameter groups: %s", err))
		d.logger.Error("asyncDeleteDB: CleanupCustomParameterGroups error", "err", err)
		return
	}

	err = d.db.Unscoped().Delete(i).Error
	if err != nil {
		d.logger.Error("asyncDeleteDB: error deleting record", "err", err)
		return
	}

	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted database resources")
}

func (d *dedicatedDBAdapter) deleteDB(i *RDSInstance) (base.InstanceState, error) {
	err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.DeleteOp, base.InstanceInProgress, "Deleting database resources")
	if err != nil {
		return base.InstanceNotGone, err
	}

	go d.asyncDeleteDB(i)

	return base.InstanceInProgress, nil
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

func getRetryMultiplier(storageSize int64) int64 {
	// Scale the number of retries in proportion to the database
	// storage size
	return max(int64(math.Ceil(float64(storageSize)/200)), 1)
}

func getPollAwsMaxWaitTime(storageSize int64, initialMaxWaitTimeDuration time.Duration) time.Duration {
	return initialMaxWaitTimeDuration * time.Duration(getRetryMultiplier(storageSize))
}

func getPollAwsMaxRetries(storageSize int64, defaultMaxRetries int64) int {
	return int(defaultMaxRetries * getRetryMultiplier(storageSize))
}
