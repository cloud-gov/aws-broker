package rds

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/base"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	brokerErrs "github.com/cloud-gov/aws-broker/errors"
	"github.com/cloud-gov/aws-broker/taskqueue"

	"errors"
	"fmt"
)

type dbAdapter interface {
	createDB(i *RDSInstance, password string, queue *taskqueue.QueueManager) (base.InstanceState, error)
	modifyDB(i *RDSInstance, password string) (base.InstanceState, error)
	checkDBStatus(i *RDSInstance) (base.InstanceState, error)
	bindDBToApp(i *RDSInstance, password string) (map[string]string, error)
	deleteDB(i *RDSInstance) (base.InstanceState, error)
}

// MockDBAdapter is a struct meant for testing.
// It should only be used in *_test.go files.
// It is only here because *_test.go files are only compiled during "go test"
// and it's referenced in non *_test.go code eg. InitializeAdapter in main.go.
type mockDBAdapter struct {
}

func (d *mockDBAdapter) createDB(i *RDSInstance, password string, queue *taskqueue.QueueManager) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockDBAdapter) modifyDB(i *RDSInstance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockDBAdapter) checkDBStatus(i *RDSInstance) (base.InstanceState, error) {
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

// END MockDBAdpater

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
		DBSubnetGroupName:          &i.DbSubnetGroup,
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

func (d *dedicatedDBAdapter) waitAndCreateDBReadReplica(i *RDSInstance, jobstate chan taskqueue.AsyncJobMsg) {
	defer close(jobstate)

	msg := taskqueue.AsyncJobMsg{
		BrokerId:   i.ServiceID,
		InstanceId: i.Uuid,
		JobType:    base.CreateOp,
		JobState:   taskqueue.AsyncJobState{},
	}

	msg.JobState.Message = fmt.Sprintf("Waiting for database creation to finish on service instance: %s", i.Uuid)
	msg.JobState.State = base.InstanceInProgress
	jobstate <- msg

	// TODO: limit loop to specific number of executions
	for {
		dbState, err := d.checkDBStatus(i)
		if err != nil {
			msg.JobState.Message = fmt.Sprintf("Failed to get database status on instance %s: %s", i.Uuid, err)
			msg.JobState.State = base.InstanceNotCreated
			jobstate <- msg
			return
		}
		if dbState == base.InstanceReady {
			break
		}
	}

	msg.JobState.Message = fmt.Sprintf("Creating database read replica for service instance: %s", i.Uuid)
	msg.JobState.State = base.InstanceInProgress
	jobstate <- msg

	err := d.createDBReadReplica(i)
	if err != nil {
		msg.JobState.Message = fmt.Sprintf("Creating database read replica on instance %s failed: %s", i.Uuid, err)
		msg.JobState.State = base.InstanceNotCreated
		jobstate <- msg
		return
	}

	msg.JobState.Message = fmt.Sprintf("Database provisioning finished for service instance: %s", i.Uuid)
	msg.JobState.State = base.InstanceGone
	jobstate <- msg
}

func (d *dedicatedDBAdapter) createDB(i *RDSInstance, password string, queue *taskqueue.QueueManager) (base.InstanceState, error) {
	createDbInputParams, err := d.prepareCreateDbInput(i, password)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	_, err = d.rds.CreateDBInstance(createDbInputParams)
	if err != nil {
		brokerErrs.LogAWSError(err)
		return base.InstanceNotCreated, err
	}

	jobchan, err := queue.RequestTaskQueue(i.ServiceID, i.Uuid, base.CreateOp)
	if err == nil {
		go d.waitAndCreateDBReadReplica(i, jobchan)
	}

	return base.InstanceInProgress, nil
}

// This should ultimately get exposed as part of the "update-service" method for the broker:
// cf update-service SERVICE_INSTANCE [-p NEW_PLAN] [-c PARAMETERS_AS_JSON] [-t TAGS] [--upgrade]
func (d *dedicatedDBAdapter) modifyDB(i *RDSInstance, password string) (base.InstanceState, error) {
	params, err := d.prepareModifyDbInstanceInput(i)
	if err != nil {
		return base.InstanceNotModified, err
	}

	_, err = d.rds.ModifyDBInstance(params)
	if err != nil {
		brokerErrs.LogAWSError(err)
		return base.InstanceNotModified, err
	}

	// if i.ReplicaDatabase != "" {
	// 	err = d.createDBReadReplica(i)
	// 	if err != nil {
	// 		return base.InstanceNotCreated, err
	// 	}
	// }

	return base.InstanceInProgress, nil
}

func (d *dedicatedDBAdapter) checkDBStatus(i *RDSInstance) (base.InstanceState, error) {
	// First, we need to check if the instance is up and available.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(i.Database),
		}

		resp, err := d.rds.DescribeDBInstances(params)
		if err != nil {
			brokerErrs.LogAWSError(err)
			return base.InstanceNotCreated, err
		}

		// Get the details (host and port) for the instance.
		numOfInstances := len(resp.DBInstances)
		if numOfInstances == 0 {
			return base.InstanceNotCreated, errors.New("could not find any instances")
		}
		for _, value := range resp.DBInstances {
			// First check that the instance is up.
			fmt.Println("Database Instance:" + i.Database + " is " + *(value.DBInstanceStatus))
			switch *(value.DBInstanceStatus) {
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
	}

	return base.InstanceNotCreated, nil
}

func (d *dedicatedDBAdapter) bindDBToApp(i *RDSInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(i.Database),
			// MaxRecords: aws.Long(1),
		}

		resp, err := d.rds.DescribeDBInstances(params)
		if err != nil {
			brokerErrs.LogAWSError(err)
			return nil, err
		}

		// Get the details (host and port) for the instance.
		numOfInstances := len(resp.DBInstances)
		if numOfInstances > 0 {
			for _, value := range resp.DBInstances {
				// First check that the instance is up.
				if value.DBInstanceStatus != nil && *(value.DBInstanceStatus) == "available" {
					if value.Endpoint != nil && value.Endpoint.Address != nil && value.Endpoint.Port != nil {
						fmt.Printf("host: %s port: %d \n", *(value.Endpoint.Address), *(value.Endpoint.Port))
						i.Port = *(value.Endpoint.Port)
						i.Host = *(value.Endpoint.Address)
						i.State = base.InstanceReady
						// Should only be one regardless. Just return now.
						break
					} else {
						// Something went horribly wrong. Should never get here.
						return nil, errors.New("Inavlid memory for endpoint and/or endpoint members.")
					}
				} else {
					// Instance not up yet.
					return nil, errors.New("Instance not available yet. Please wait and try again..")
				}
			}
		} else {
			// Couldn't find any instances.
			return nil, errors.New("Couldn't find any instances.")
		}
	}
	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(password)
}

func (d *dedicatedDBAdapter) deleteDB(i *RDSInstance) (base.InstanceState, error) {
	params := &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(i.Database), // Required
		// FinalDBSnapshotIdentifier: aws.String("String"),
		DeleteAutomatedBackups: aws.Bool(false),
		SkipFinalSnapshot:      aws.Bool(true),
	}
	_, err := d.rds.DeleteDBInstance(params)

	if err != nil {
		brokerErrs.LogAWSError(err)
		return base.InstanceNotGone, err
	}

	// clean up custom parameter groups
	d.parameterGroupClient.CleanupCustomParameterGroups()
	return base.InstanceGone, nil
}
