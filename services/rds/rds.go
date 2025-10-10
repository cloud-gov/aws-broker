package rds

import (
	"context"
	"time"

	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/awserr"
	// "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/cloud-gov/aws-broker/base"
	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	brokerErrs "github.com/cloud-gov/aws-broker/errors"
	jobs "github.com/cloud-gov/aws-broker/jobs"

	"errors"
	"fmt"
)

type dbAdapter interface {
	createDB(i *RDSInstance, password string) (base.InstanceState, error)
	modifyDB(i *RDSInstance) (base.InstanceState, error)
	checkDBStatus(database string) (base.InstanceState, error)
	bindDBToApp(i *RDSInstance, password string) (map[string]string, error)
	deleteDB(i *RDSInstance) (base.InstanceState, error)
	describeDatabaseInstance(database string) (*rdsTypes.DBInstance, error)
}

// MockDBAdapter is a struct meant for testing.
// It should only be used in *_test.go files.
// It is only here because *_test.go files are only compiled during "go test"
// and it's referenced in non *_test.go code eg. InitializeAdapter in main.go.
type mockDBAdapter struct {
	createDBState *base.InstanceState
}

func (d *mockDBAdapter) createDB(i *RDSInstance, password string) (base.InstanceState, error) {
	// TODO
	if d.createDBState != nil {
		return *d.createDBState, nil
	}
	return base.InstanceInProgress, nil
}

func (d *mockDBAdapter) modifyDB(i *RDSInstance) (base.InstanceState, error) {
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

func (d *mockDBAdapter) deleteDB(i *RDSInstance) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
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
	Plan                 catalog.RDSPlan
	settings             config.Settings
	rds                  RDSClientInterface
	parameterGroupClient parameterGroupClient
	db                   *gorm.DB
}

func (d *dedicatedDBAdapter) prepareCreateDbInput(
	i *RDSInstance,
	password string,
) (*rds.CreateDBInstanceInput, error) {
	rdsTags := ConvertTagsToRDSTags(i.Tags)

	// Standard parameters
	params := &rds.CreateDBInstanceInput{
		AllocatedStorage: aws.Int32(i.AllocatedStorage),
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
		BackupRetentionPeriod:   aws.Int32(i.BackupRetentionPeriod),
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
	err := d.parameterGroupClient.ProvisionCustomParameterGroupIfNecessary(i, rdsTags)
	if err != nil {
		return nil, err
	}
	if i.ParameterGroupName != "" {
		params.DBParameterGroupName = aws.String(i.ParameterGroupName)
	}

	return params, nil
}

func (d *dedicatedDBAdapter) prepareModifyDbInstanceInput(i *RDSInstance, database string) (*rds.ModifyDBInstanceInput, error) {
	// Standard parameters (https://docs.aws.amazon.com/sdk-for-go/api/service/rds/#RDS.ModifyDBInstance)
	// These actions are applied immediately.
	params := &rds.ModifyDBInstanceInput{
		AllocatedStorage:         aws.Int32(i.AllocatedStorage),
		ApplyImmediately:         aws.Bool(true),
		DBInstanceClass:          &d.Plan.InstanceClass,
		MultiAZ:                  &d.Plan.Redundant,
		DBInstanceIdentifier:     &database,
		AllowMajorVersionUpgrade: aws.Bool(false),
		BackupRetentionPeriod:    aws.Int32(i.BackupRetentionPeriod),
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

func (d *dedicatedDBAdapter) createDBReadReplica(i *RDSInstance) (*rds.CreateDBInstanceReadReplicaOutput, error) {
	rdsTags := ConvertTagsToRDSTags(i.Tags)
	createReadReplicaParams := &rds.CreateDBInstanceReadReplicaInput{
		AutoMinorVersionUpgrade:    aws.Bool(true),
		DBInstanceIdentifier:       &i.ReplicaDatabase,
		SourceDBInstanceIdentifier: &i.Database,
		MultiAZ:                    &d.Plan.Redundant,
		PubliclyAccessible:         aws.Bool(d.settings.PubliclyAccessibleFeature && i.PubliclyAccessible),
		StorageType:                aws.String(i.StorageType),
		Tags:                       rdsTags,
		VpcSecurityGroupIds: []string{
			i.SecGroup,
		},
	}
	return d.rds.CreateDBInstanceReadReplica(context.TODO(), createReadReplicaParams)
}

func (d *dedicatedDBAdapter) waitForDbReady(operation base.Operation, i *RDSInstance, database string) error {
	attempt := 1
	var dbState base.InstanceState
	var err error

	for attempt <= int(d.settings.PollAwsMaxRetries) {
		dbState, err = d.checkDBStatus(database)
		if err != nil {
			updateErr := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Failed to get database status: %s", err))
			if updateErr != nil {
				err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
			}
			return fmt.Errorf("waitForDbReady: %w", err)
		}

		if dbState == base.InstanceReady {
			return nil
		}

		err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Waiting for database to be available. Current status: %s (attempt %d of %d)", dbState, attempt, d.settings.PollAwsMaxRetries))
		if err != nil {
			return fmt.Errorf("waitForDbReady: %w", err)
		}

		attempt += 1
		time.Sleep(time.Duration(d.settings.PollAwsRetryDelaySeconds) * time.Second)
	}

	if dbState != base.InstanceReady {
		err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, "Exhausted maximum retries waiting for database to be available")
		if err != nil {
			return fmt.Errorf("waitForDbReady: %w", err)
		}
		return errors.New("waitForDbReady: exhausted maximum retries waiting for database to be available")
	}

	return nil
}

func (d *dedicatedDBAdapter) updateDBTags(i *RDSInstance, dbInstanceARN string) error {
	_, err := d.rds.AddTagsToResource(context.TODO(), &rds.AddTagsToResourceInput{
		ResourceName: aws.String(dbInstanceARN),
		Tags:         ConvertTagsToRDSTags(i.Tags),
	})
	return err
}

func (d *dedicatedDBAdapter) waitAndCreateDBReadReplica(operation base.Operation, i *RDSInstance) error {
	jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Creating database read replica")

	createReplicaOutput, err := d.createDBReadReplica(i)
	if err != nil {
		fmt.Println(err)
		jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Creating database read replica failed: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = d.waitForDbReady(operation, i, i.ReplicaDatabase)
	if err != nil {
		fmt.Println(err)
		jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for replica database to become available: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	err = d.updateDBTags(i, *createReplicaOutput.DBInstance.DBInstanceArn)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("waitAndCreateDBReadReplica: %w", err)
	}

	return nil
}

func (d *dedicatedDBAdapter) asyncCreateDB(i *RDSInstance, password string) {
	operation := base.CreateOp

	createDbInputParams, err := d.prepareCreateDbInput(i, password)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error generating database creation params: %s", err))
		fmt.Printf("asyncCreateDB: %s\n", err)
		return
	}

	_, err = d.rds.CreateDBInstance(context.TODO(), createDbInputParams)
	if err != nil {
		brokerErrs.LogAWSError(err)
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error creating database: %s", err))
		fmt.Printf("asyncCreateDB: %s\n", err)
		return
	}

	err = d.waitForDbReady(operation, i, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error waiting for database to become available: %s", err))
		fmt.Printf("asyncCreateDB: %s\n", err)
		return
	}

	if i.AddReadReplica {
		err := d.waitAndCreateDBReadReplica(operation, i)
		if err != nil {
			jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Error creating database replica: %s", err))
			fmt.Printf("asyncCreateDB: %s\n", err)
			return
		}
	}

	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished creating database resources")
}

func (d *dedicatedDBAdapter) createDB(i *RDSInstance, password string) (base.InstanceState, error) {
	err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.CreateOp, base.InstanceInProgress, "Database creation in progress")
	if err != nil {
		return base.InstanceNotCreated, err
	}

	go d.asyncCreateDB(i, password)

	return base.InstanceInProgress, nil
}

func (d *dedicatedDBAdapter) asyncModifyDbInstance(operation base.Operation, i *RDSInstance, database string) error {
	modifyParams, err := d.prepareModifyDbInstanceInput(i, database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error preparing database modify parameters: %s", err))
		return fmt.Errorf("asyncModifyDb, error preparing modify database input: %w", err)
	}

	modifyReplicaOutput, err := d.rds.ModifyDBInstance(context.TODO(), modifyParams)
	if err != nil {
		brokerErrs.LogAWSError(err)
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database: %s", err))
		return fmt.Errorf("asyncModifyDb, error modifying database instance: %w", err)
	}

	err = d.waitForDbReady(operation, i, database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error waiting for database to become available: %s", err))
		return fmt.Errorf("asyncModifyDb, error waiting for database to be ready: %w", err)
	}

	err = d.updateDBTags(i, *modifyReplicaOutput.DBInstance.DBInstanceArn)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error updating tags for database replica: %s", err))
		return fmt.Errorf("asyncModifyDb, error updating replica tags: %w", err)
	}

	return nil
}

func (d *dedicatedDBAdapter) asyncModifyDb(i *RDSInstance) {
	operation := base.ModifyOp

	err := d.asyncModifyDbInstance(operation, i, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database: %s", err))
		fmt.Printf("asyncModifyDb: %s\n", err)
		return
	}

	if i.AddReadReplica {
		// Add new read replica
		err := d.waitAndCreateDBReadReplica(operation, i)
		if err != nil {
			jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error creating database replica: %s", err))
			fmt.Printf("asyncModifyDb, error creating read replica: %s\n", err)
			return
		}
	} else if !i.DeleteReadReplica && !i.AddReadReplica && i.ReplicaDatabase != "" {
		err := d.asyncModifyDbInstance(operation, i, i.ReplicaDatabase)
		if err != nil {
			jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error modifying database replica: %s", err))
			fmt.Printf("asyncModifyDb, error modifying read replica: %s\n", err)
			return
		}
	}

	if i.DeleteReadReplica {
		err = d.deleteDatabaseReadReplica(i, operation)
		if err != nil {
			jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error deleting database replica: %s", err))
			fmt.Printf("asyncModifyDb, error deleting database replica: %s\n", err)
			return
		}
	}

	err = d.db.Save(i).Error
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotModified, fmt.Sprintf("Error saving record: %s", err))
		fmt.Printf("asyncModifyDb, error saving record: %s\n", err)
		return
	}

	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished modifying database resources")
}

// This should ultimately get exposed as part of the "update-service" method for the broker:
// cf update-service SERVICE_INSTANCE [-p NEW_PLAN] [-c PARAMETERS_AS_JSON] [-t TAGS] [--upgrade]
func (d *dedicatedDBAdapter) modifyDB(i *RDSInstance) (base.InstanceState, error) {
	err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.ModifyOp, base.InstanceInProgress, "Database modification in progress")
	if err != nil {
		return base.InstanceNotModified, err
	}

	go d.asyncModifyDb(i)

	return base.InstanceInProgress, nil
}

func (d *dedicatedDBAdapter) describeDatabaseInstance(database string) (*rdsTypes.DBInstance, error) {
	params := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(database),
	}

	resp, err := d.rds.DescribeDBInstances(context.TODO(), params)
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
	attempt := 1
	var dbState base.InstanceState
	var err error
	var isDeleted bool

	for !isDeleted && attempt <= int(d.settings.PollAwsMaxRetries) {
		dbState, err = d.checkDBStatus(database)
		if err != nil {
			var exception *rdsTypes.DBInstanceNotFoundFault
			if errors.As(err, &exception) {
				updateErr := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("Could not check database status: %s", err))
				if updateErr != nil {
					err = fmt.Errorf("waitForDbDeleted: while handling error %w, error updating async job message %w", err, updateErr)
				}
				return fmt.Errorf("waitForDbDeleted: checkDBStatus err %w", err)
			}

			isDeleted = true
			break
		}

		err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Waiting for database to be be deleted. Current status: %s (attempt %d of %d)", dbState, attempt, d.settings.PollAwsMaxRetries))
		if err != nil {
			return fmt.Errorf("waitForDbDeleted: %w", err)
		}

		attempt += 1
		time.Sleep(time.Duration(d.settings.PollAwsRetryDelaySeconds) * time.Second)
	}

	if !isDeleted {
		err := jobs.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, "Exhausted maximum retries waiting for database to be deleted")
		if err != nil {
			return fmt.Errorf("waitForDbDeleted: %w", err)
		}
		return errors.New("waitForDbDeleted: exhausted maximum retries waiting for database to be deleted")
	}

	return nil
}

func (d *dedicatedDBAdapter) deleteDatabaseReadReplica(i *RDSInstance, operation base.Operation) error {
	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")

	params := prepareDeleteDbInput(i.ReplicaDatabase)
	_, err := d.rds.DeleteDBInstance(context.TODO(), params)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Failed to delete replica database: %s", err))
		return fmt.Errorf("deleteDatabaseReadReplica: %w", err)
	}

	err = d.waitForDbDeleted(operation, i, i.ReplicaDatabase)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Failed to confirm replica database deletion: %s", err))
		return fmt.Errorf("deleteDatabaseReadReplica: %w", err)
	}

	i.ReplicaDatabase = ""

	return nil
}

func (d *dedicatedDBAdapter) asyncDeleteDB(i *RDSInstance) {
	operation := base.DeleteOp

	if i.ReplicaDatabase != "" {
		d.deleteDatabaseReadReplica(i, operation)
	}

	params := prepareDeleteDbInput(i.Database)
	_, err := d.rds.DeleteDBInstance(context.TODO(), params)
	if err != nil {
		brokerErrs.LogAWSError(err)
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Failed to delete database: %s", err))
		fmt.Printf("asyncDeleteDB: %s\n", err)
		return
	}

	err = d.waitForDbDeleted(operation, i, i.Database)
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Failed to confirm database deletion: %s", err))
		fmt.Printf("asyncDeleteDB: %s\n", err)
		return
	}

	jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up parameter groups")
	err = d.parameterGroupClient.CleanupCustomParameterGroups()
	if err != nil {
		jobs.ShouldWriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, fmt.Sprintf("Failed to cleanup parameter groups: %s", err))
		fmt.Printf("asyncDeleteDB: %s\n", err)
		return
	}

	err = d.db.Unscoped().Delete(i).Error
	if err != nil {
		fmt.Println(fmt.Errorf("asyncDeleteDB, error deleting record: %w", err))
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
