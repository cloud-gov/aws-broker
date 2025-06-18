package rds

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/jinzhu/gorm"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	brokerErrs "github.com/cloud-gov/aws-broker/errors"
	"github.com/cloud-gov/aws-broker/taskqueue"

	"errors"
	"fmt"
)

type dbAdapter interface {
	createDB(i *RDSInstance, password string, db *gorm.DB) (base.InstanceState, error)
	modifyDB(i *RDSInstance, password string, db *gorm.DB) (base.InstanceState, error)
	checkDBStatus(database string) (base.InstanceState, error)
	bindDBToApp(i *RDSInstance, password string) (map[string]string, error)
	deleteDB(i *RDSInstance, db *gorm.DB) (base.InstanceState, error)
	describeDatabaseInstance(database string) (*rds.DBInstance, error)
}

// MockDBAdapter is a struct meant for testing.
// It should only be used in *_test.go files.
// It is only here because *_test.go files are only compiled during "go test"
// and it's referenced in non *_test.go code eg. InitializeAdapter in main.go.
type mockDBAdapter struct {
	createDBState *base.InstanceState
}

func (d *mockDBAdapter) createDB(i *RDSInstance, password string, db *gorm.DB) (base.InstanceState, error) {
	// TODO
	if d.createDBState != nil {
		return *d.createDBState, nil
	}
	return base.InstanceInProgress, nil
}

func (d *mockDBAdapter) modifyDB(i *RDSInstance, password string, db *gorm.DB) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *mockDBAdapter) checkDBStatus(database string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockDBAdapter) bindDBToApp(i *RDSInstance, password string) (map[string]string, error) {
	// TODO
	return i.getCredentials(password)
}

func (d *mockDBAdapter) deleteDB(i *RDSInstance, db *gorm.DB) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}

func (d *mockDBAdapter) describeDatabaseInstance(database string) (*rds.DBInstance, error) {
	return nil, nil
}

// END MockDBAdpater
type DBEndpointDetails struct {
	Port  int64
	Host  string
	State base.InstanceState
}

type dedicatedDBAdapter struct {
	Plan                 catalog.RDSPlan
	settings             config.Settings
	rds                  rdsiface.RDSAPI
	parameterGroupClient parameterGroupClient
}

func (d *dedicatedDBAdapter) prepareCreateDbInput(
	i *RDSInstance,
	password string,
) (*rds.CreateDBInstanceInput, error) {
	rdsTags := ConvertTagsToRDSTags(i.Tags)

	// Standard parameters
	params := &rds.CreateDBInstanceInput{
		AllocatedStorage: aws.Int64(i.AllocatedStorage),
		// Instance class is defined by the plan
		DBInstanceClass:         &d.Plan.InstanceClass,
		DBInstanceIdentifier:    &i.Database,
		DBName:                  aws.String(i.FormatDBName()),
		Engine:                  aws.String(i.DbType),
		MasterUserPassword:      &password,
		MasterUsername:          &i.Username,
		AutoMinorVersionUpgrade: aws.Bool(true),
		MultiAZ:                 aws.Bool(d.Plan.Redundant),
		StorageEncrypted:        aws.Bool(d.Plan.Encrypted),
		StorageType:             aws.String(i.StorageType),
		Tags:                    rdsTags,
		PubliclyAccessible:      aws.Bool(d.settings.PubliclyAccessibleFeature && i.PubliclyAccessible),
		BackupRetentionPeriod:   aws.Int64(i.BackupRetentionPeriod),
		DBSubnetGroupName:       &i.DbSubnetGroup,
		VpcSecurityGroupIds: []*string{
			&i.SecGroup,
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
	err := d.parameterGroupClient.ProvisionCustomParameterGroupIfNecessary(i, rdsTags)
	if err != nil {
		return nil, err
	}
	if i.ParameterGroupName != "" {
		params.DBParameterGroupName = aws.String(i.ParameterGroupName)
	}

	return params, nil
}

func (d *dedicatedDBAdapter) prepareModifyDbInstanceInput(i *RDSInstance) (*rds.ModifyDBInstanceInput, error) {
	// Standard parameters (https://docs.aws.amazon.com/sdk-for-go/api/service/rds/#RDS.ModifyDBInstance)
	// These actions are applied immediately.
	params := &rds.ModifyDBInstanceInput{
		AllocatedStorage:         aws.Int64(i.AllocatedStorage),
		ApplyImmediately:         aws.Bool(true),
		DBInstanceClass:          &d.Plan.InstanceClass,
		MultiAZ:                  &d.Plan.Redundant,
		DBInstanceIdentifier:     &i.Database,
		AllowMajorVersionUpgrade: aws.Bool(false),
		BackupRetentionPeriod:    aws.Int64(i.BackupRetentionPeriod),
	}

	if i.StorageType != "" {
		params.StorageType = aws.String(i.StorageType)
	}

	if i.ClearPassword != "" {
		params.MasterUserPassword = aws.String(i.ClearPassword)
	}

	rdsTags := ConvertTagsToRDSTags(i.Tags)

	// If a custom parameter has been requested, and the feature is enabled,
	// create/update a custom parameter group for our custom parameters.
	err := d.parameterGroupClient.ProvisionCustomParameterGroupIfNecessary(i, rdsTags)
	if err != nil {
		return nil, err
	}
	if i.ParameterGroupName != "" {
		params.DBParameterGroupName = aws.String(i.ParameterGroupName)
	}
	return params, nil
}

func (d *dedicatedDBAdapter) createDBReadReplica(i *RDSInstance) error {
	rdsTags := ConvertTagsToRDSTags(i.Tags)
	createReadReplicaParams := &rds.CreateDBInstanceReadReplicaInput{
		AutoMinorVersionUpgrade:    aws.Bool(true),
		DBInstanceIdentifier:       &i.ReplicaDatabase,
		SourceDBInstanceIdentifier: &i.Database,
		MultiAZ:                    &d.Plan.Redundant,
		PubliclyAccessible:         aws.Bool(d.settings.PubliclyAccessibleFeature && i.PubliclyAccessible),
		StorageType:                aws.String(i.StorageType),
		Tags:                       rdsTags,
		VpcSecurityGroupIds: []*string{
			&i.SecGroup,
		},
	}
	if i.ParameterGroupName != "" {
		createReadReplicaParams.DBParameterGroupName = aws.String(i.ParameterGroupName)
	}
	_, err := d.rds.CreateDBInstanceReadReplica(createReadReplicaParams)
	return err
}

func (d *dedicatedDBAdapter) waitForDbReady(db *gorm.DB, operation base.Operation, i *RDSInstance, database string) error {
	attempt := 1
	var dbState base.InstanceState
	var err error

	for attempt <= int(d.settings.PollAwsMaxRetries) {
		dbState, err = d.checkDBStatus(database)
		if err != nil {
			updateErr := taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Failed to get database status: %s", err))
			if updateErr != nil {
				return updateErr
			}
			return err
		}

		if dbState == base.InstanceReady {
			return nil
		}

		err := taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Waiting for database to be available. Current status: %s (attempt %d of %d)", dbState, attempt, d.settings.PollAwsMaxRetries))
		if err != nil {
			return err
		}

		attempt += 1
		time.Sleep(time.Duration(d.settings.PollAwsRetryDelaySeconds) * time.Second)
	}

	if dbState != base.InstanceReady {
		err := taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, "Exhausted maximum retries waiting for database to be available")
		if err != nil {
			return err
		}
		return errors.New("exhausted maximum retries waiting for database to be available")
	}

	return nil
}

func (d *dedicatedDBAdapter) waitAndCreateDBReadReplica(db *gorm.DB, operation base.Operation, i *RDSInstance) {
	err := d.waitForDbReady(db, operation, i, i.Database)
	if err != nil {
		taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return
	}

	taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Creating database read replica")

	err = d.createDBReadReplica(i)
	if err != nil {
		taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Creating database read replica failed: %s", err))
		return
	}

	err = d.waitForDbReady(db, operation, i, i.ReplicaDatabase)
	if err != nil {
		taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for replica database to become available: %s", err))
		return
	}

	taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Database provisioning finished for service instance")
}

func (d *dedicatedDBAdapter) createDB(i *RDSInstance, password string, db *gorm.DB) (base.InstanceState, error) {
	createDbInputParams, err := d.prepareCreateDbInput(i, password)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	_, err = d.rds.CreateDBInstance(createDbInputParams)
	if err != nil {
		brokerErrs.LogAWSError(err)
		return base.InstanceNotCreated, err
	}

	if i.AddReadReplica {
		err := taskqueue.CreateAsyncJobMessage(db, i.ServiceID, i.Uuid, base.CreateOp, base.InstanceInProgress, "Database creation in progress")
		if err != nil {
			return base.InstanceNotCreated, err
		}
		go d.waitAndCreateDBReadReplica(db, base.CreateOp, i)
	}

	return base.InstanceInProgress, nil
}

// This should ultimately get exposed as part of the "update-service" method for the broker:
// cf update-service SERVICE_INSTANCE [-p NEW_PLAN] [-c PARAMETERS_AS_JSON] [-t TAGS] [--upgrade]
func (d *dedicatedDBAdapter) modifyDB(i *RDSInstance, password string, db *gorm.DB) (base.InstanceState, error) {
	params, err := d.prepareModifyDbInstanceInput(i)
	if err != nil {
		return base.InstanceNotModified, err
	}

	_, err = d.rds.ModifyDBInstance(params)
	if err != nil {
		brokerErrs.LogAWSError(err)
		return base.InstanceNotModified, err
	}

	// If we are updating to a plan that supports read replicas, but one does not already
	// exist, we need to create a read replica
	if i.AddReadReplica {
		err := taskqueue.CreateAsyncJobMessage(db, i.ServiceID, i.Uuid, base.ModifyOp, base.InstanceInProgress, "Modifying database")
		if err != nil {
			return base.InstanceNotModified, err
		}
		go d.waitAndCreateDBReadReplica(db, base.ModifyOp, i)
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedDBAdapter) describeDatabaseInstance(database string) (*rds.DBInstance, error) {
	params := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(database),
	}

	resp, err := d.rds.DescribeDBInstances(params)
	if err != nil {
		brokerErrs.LogAWSError(err)
		return nil, err
	}

	numOfInstances := len(resp.DBInstances)
	if numOfInstances == 0 {
		return nil, errors.New("could not find any instances")
	}

	if numOfInstances > 1 {
		return nil, fmt.Errorf("found more than one database for %s", database)
	}

	return resp.DBInstances[0], nil
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
		Port:  *(dbInstance.Endpoint.Port),
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

	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(password)
}

func (d *dedicatedDBAdapter) waitForDbDeleted(db *gorm.DB, operation base.Operation, i *RDSInstance, database string) error {
	attempt := 1
	var dbState base.InstanceState
	var err error
	var isDeleted bool

	for !isDeleted && attempt <= int(d.settings.PollAwsMaxRetries) {
		dbState, err = d.checkDBStatus(database)
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok || awsErr.Code() != awsRds.ErrCodeDBInstanceNotFoundFault {
				taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Could not check database status: %s", err))
				return err
			}

			isDeleted = true
			break
		}

		err := taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Waiting for database to be be deleted. Current status: %s (attempt %d of %d)", dbState, attempt, d.settings.PollAwsMaxRetries))
		if err != nil {
			return err
		}

		attempt += 1
		time.Sleep(time.Duration(d.settings.PollAwsRetryDelaySeconds) * time.Second)
	}

	if !isDeleted {
		err := taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, "Exhausted maximum retries waiting for database to be deleted")
		if err != nil {
			return err
		}
		return errors.New("exhausted maximum retries waiting for database to be deleted")
	}

	return nil
}

func (d *dedicatedDBAdapter) asyncDeleteDB(db *gorm.DB, operation base.Operation, i *RDSInstance) {
	if i.ReplicaDatabase != "" {
		taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, "Exhausted maximum retries waiting for database to be available")

		params := prepareDeleteDbInput(i.ReplicaDatabase)
		_, err := d.rds.DeleteDBInstance(params)
		if err != nil {
			taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete replica database: %s", err))
		}

		err = d.waitForDbDeleted(db, operation, i, i.ReplicaDatabase)
		if err != nil {
			taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to confirm replica database deletion: %s", err))
		}
	}

	params := prepareDeleteDbInput(i.Database)
	_, err := d.rds.DeleteDBInstance(params)
	if err != nil {
		brokerErrs.LogAWSError(err)
		taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to delete database: %s", err))
	}

	err = d.waitForDbDeleted(db, operation, i, i.Database)
	if err != nil {
		taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Failed to confirm database deletion: %s", err))
	}

	taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up parameter groups")
	d.parameterGroupClient.CleanupCustomParameterGroups()

	taskqueue.UpdateAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted database resources")
}

func (d *dedicatedDBAdapter) deleteDB(i *RDSInstance, db *gorm.DB) (base.InstanceState, error) {
	err := taskqueue.CreateAsyncJobMessage(db, i.ServiceID, i.Uuid, base.DeleteOp, base.InstanceInProgress, "Deleting database resources")
	if err != nil {
		return base.InstanceNotModified, err
	}

	go d.asyncDeleteDB(db, base.DeleteOp, i)

	return base.InstanceInProgress, nil
}

func prepareDeleteDbInput(database string) *rds.DeleteDBInstanceInput {
	return &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   aws.String(database), // Required
		DeleteAutomatedBackups: aws.Bool(false),
		SkipFinalSnapshot:      aws.Bool(true),
	}
}
