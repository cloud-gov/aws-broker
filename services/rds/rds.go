package rds

import (
	"github.com/18F/aws-broker/base"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"

	"errors"
	"fmt"
)

type dbAdapter interface {
	createDB(i *RDSInstance, password string) (base.InstanceState, error)
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

func (d *mockDBAdapter) createDB(i *RDSInstance, password string) (base.InstanceState, error) {
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

type sharedDBAdapter struct {
	SharedDbConn *gorm.DB
}

func (d *sharedDBAdapter) createDB(i *RDSInstance, password string) (base.InstanceState, error) {
	dbName := i.FormatDBName()
	switch i.DbType {
	case "postgres":
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName)); db.Error != nil {
			return base.InstanceNotCreated, db.Error
		}
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", i.Username, password)); db.Error != nil {
			// TODO. Revert CREATE DATABASE.
			return base.InstanceNotCreated, db.Error
		}
		if db := d.SharedDbConn.Exec(fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s;", dbName, i.Username)); db.Error != nil {
			// TODO. Revert CREATE DATABASE and CREATE USER.
			return base.InstanceNotCreated, db.Error
		}
	case "mysql":
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName)); db.Error != nil {
			return base.InstanceNotCreated, db.Error
		}
		// Double % escapes to one %.
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';", i.Username, password)); db.Error != nil {
			// TODO. Revert CREATE DATABASE.
			return base.InstanceNotCreated, db.Error
		}
		// Double % escapes to one %.
		if db := d.SharedDbConn.Exec(fmt.Sprintf("GRANT ALL ON %s.* TO '%s'@'%%';", dbName, i.Username)); db.Error != nil {
			// TODO. Revert CREATE DATABASE and CREATE USER.
			return base.InstanceNotCreated, db.Error
		}
	default:
		return base.InstanceNotCreated, fmt.Errorf("Unsupported database type: %s, cannot create shared database", i.DbType)
	}
	return base.InstanceReady, nil
}

func (d *sharedDBAdapter) modifyDB(i *RDSInstance, password string) (base.InstanceState, error) {
	return base.InstanceNotModified, nil
}

func (d *sharedDBAdapter) checkDBStatus(i *RDSInstance) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *sharedDBAdapter) bindDBToApp(i *RDSInstance, password string) (map[string]string, error) {
	return i.getCredentials(password)
}

func (d *sharedDBAdapter) deleteDB(i *RDSInstance) (base.InstanceState, error) {
	if db := d.SharedDbConn.Exec(fmt.Sprintf("DROP DATABASE %s;", i.FormatDBName())); db.Error != nil {
		return base.InstanceNotGone, db.Error
	}
	if db := d.SharedDbConn.Exec(fmt.Sprintf("DROP USER %s;", i.Username)); db.Error != nil {
		return base.InstanceNotGone, db.Error
	}
	return base.InstanceGone, nil
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
	var rdsTags []*rds.Tag

	for k, v := range i.Tags {
		tag := rds.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		rdsTags = append(rdsTags, &tag)
	}

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
		StorageType:             aws.String(d.Plan.StorageType),
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
	err := d.parameterGroupClient.ProvisionCustomParameterGroupIfNecessary(i)
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
	// NOTE:  Only the following actions are allowed at this point:
	// - Instance class modification (change of plan)
	// - Multi AZ (redundancy)
	// - Allocated storage
	// These actions are applied immediately.
	params := &rds.ModifyDBInstanceInput{
		AllocatedStorage:         aws.Int64(i.AllocatedStorage),
		ApplyImmediately:         aws.Bool(true),
		DBInstanceClass:          &d.Plan.InstanceClass,
		MultiAZ:                  &d.Plan.Redundant,
		DBInstanceIdentifier:     &i.Database,
		AllowMajorVersionUpgrade: aws.Bool(false),
		BackupRetentionPeriod:    aws.Int64(i.BackupRetentionPeriod),
		MasterUserPassword:       aws.String(i.ClearPassword),
	}

	// If a custom parameter has been requested, and the feature is enabled,
	// create/update a custom parameter group for our custom parameters.
	err := d.parameterGroupClient.ProvisionCustomParameterGroupIfNecessary(i)
	if err != nil {
		return nil, err
	}
	if i.ParameterGroupName != "" {
		params.DBParameterGroupName = aws.String(i.ParameterGroupName)
	}
	return params, nil
}

func (d *dedicatedDBAdapter) createDB(i *RDSInstance, password string) (base.InstanceState, error) {
	params, err := d.prepareCreateDbInput(i, password)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	_, err = d.rds.CreateDBInstance(params)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
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
		return base.InstanceNotModified, err
	}

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}

	return base.InstanceNotModified, nil
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
			if awsErr, ok := err.(awserr.Error); ok {
				// Generic AWS error with Code, Message, and original error (if any)
				fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// A service error occurred
					fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				}
			} else {
				// This case should never be hit, the SDK should always return an
				// error which satisfies the awserr.Error interface.
				fmt.Println(err.Error())
			}
			return base.InstanceNotCreated, err
		}

		// Get the details (host and port) for the instance.
		numOfInstances := len(resp.DBInstances)
		if numOfInstances > 0 {
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
		} else {
			// Couldn't find any instances.
			return base.InstanceNotCreated, errors.New("Couldn't find any instances.")
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
			if awsErr, ok := err.(awserr.Error); ok {
				// Generic AWS error with Code, Message, and original error (if any)
				fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// A service error occurred
					fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				}
			} else {
				// This case should never be hit, the SDK should always return an
				// error which satisfies the awserr.Error interface.
				fmt.Println(err.Error())
			}
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

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		// clean up custom parameter groups
		d.parameterGroupClient.CleanupCustomParameterGroups()
		return base.InstanceGone, nil
	}
	return base.InstanceNotGone, nil
}

func (d *dedicatedDBAdapter) didAwsCallSucceed(err error) bool {
	// TODO Eventually return a formatted error object.
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
		return false
	}
	return true
}
