package rds

import (
	"github.com/18F/aws-broker/base"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"

	"errors"
	"fmt"
	"log"
	"regexp"
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
	Plan     catalog.RDSPlan
	settings config.Settings
}

// PgroupPrefix is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-aws-broker-"

// This function will return the a custom parameter group with whatever custom
// parameters have been requested.  If there is no custom parameter group, it
// will be created.
func getCustomParameterGroup(pgroupName string, i *RDSInstance, customparams map[string]map[string]string, svc *rds.RDS) (string, error) {
	dbParametersInput := &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(pgroupName),
		MaxRecords:           aws.Int64(20),
		Source:               aws.String("system"),
	}

	// If the db parameter group has already been created, we can return.
	_, err := svc.DescribeDBParameters(dbParametersInput)
	if err == nil {
		log.Printf("%s parameter group already exists", pgroupName)
	} else {
		// Otherwise, create a new parameter group in the proper family.
		pgroupFamily := ""

		// If the DB version is not set (e.g., creating a new instance without
		// providing a specific version), determine the default parameter group
		// name from the default engine that will be chosen.
		if i.DbVersion == "" {
			dbEngineVersionsInput := &rds.DescribeDBEngineVersionsInput{
				DefaultOnly: aws.Bool(true),
				Engine:      aws.String(i.DbType),
			}

			// This call requires that the broker have permissions to make it.
			defaultEngineInfo, err := svc.DescribeDBEngineVersions(dbEngineVersionsInput)

			if err != nil {
				return "Error retrieving default parameter group name", err
			}

			// The value from the engine info is a string pointer, so we must
			// retrieve its actual value.
			pgroupFamily = *defaultEngineInfo.DBEngineVersions[0].DBParameterGroupFamily
		} else {
			// The DB instance has a version, therefore we can derive the
			// parameter group family directly.
			re := regexp.MustCompile(`^\d+\.*\d*`)
			dbversion := re.Find([]byte(i.DbVersion))
			pgroupFamily = i.DbType + string(dbversion)
			log.Printf("creating a parameter group named %s in the family of %s", pgroupName, pgroupFamily)
		}

		createInput := &rds.CreateDBParameterGroupInput{
			DBParameterGroupFamily: aws.String(pgroupFamily),
			DBParameterGroupName:   aws.String(pgroupName),
			Description:            aws.String("aws broker parameter group for " + i.FormatDBName()),
		}

		_, err = svc.CreateDBParameterGroup(createInput)
		if err != nil {
			return pgroupName, err
		}
	}

	// iterate through the options and plug them into the parameter list
	parameters := []*rds.Parameter{}
	for k, v := range customparams[i.DbType] {
		parameters = append(parameters, &rds.Parameter{
			ApplyMethod:    aws.String("immediate"),
			ParameterName:  aws.String(k),
			ParameterValue: aws.String(v),
		})
	}

	// modify the parameter group we just created with the parameter list
	modifyinput := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(pgroupName),
		Parameters:           parameters,
	}
	_, err = svc.ModifyDBParameterGroup(modifyinput)
	if err != nil {
		return pgroupName, err
	}

	return pgroupName, nil
}

// This is here because the check is kinda big and ugly
func needCustomParameters(i *RDSInstance, s config.Settings) bool {
	// Currently, we only have one custom parameter for mysql, but if
	// we ever need to apply more, you can add them in here.
	if i.EnableFunctions &&
		s.EnableFunctionsFeature &&
		(i.DbType == "mysql") {
		return true
	}
	return false
}

func (d *dedicatedDBAdapter) createDB(i *RDSInstance, password string) (base.InstanceState, error) {
	svc := rds.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	var rdsTags []*rds.Tag

	for k, v := range i.Tags {
		var tag rds.Tag
		tag = rds.Tag{
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
	if needCustomParameters(i, d.settings) {
		customRDSParameters := make(map[string]map[string]string)

		// enable functions
		customRDSParameters["mysql"] = make(map[string]string)
		if i.EnableFunctions && d.settings.EnableFunctionsFeature {
			customRDSParameters["mysql"]["log_bin_trust_function_creators"] = "1"
		} else {
			customRDSParameters["mysql"]["log_bin_trust_function_creators"] = "0"
		}

		// Currently, we only have one custom parameter for mysql, but if
		// we ever need to apply more, you can add them in here.

		// apply parameter group
		pgroupName, err := getCustomParameterGroup(PgroupPrefix+i.FormatDBName(), i, customRDSParameters, svc)
		if err != nil {
			log.Println(err.Error())
			return base.InstanceNotCreated, nil
		}
		params.DBParameterGroupName = aws.String(pgroupName)
	}

	resp, err := svc.CreateDBInstance(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

// This should ultimately get exposed as part of the "update-service" method for the broker:
// cf update-service SERVICE_INSTANCE [-p NEW_PLAN] [-c PARAMETERS_AS_JSON] [-t TAGS] [--upgrade]
func (d *dedicatedDBAdapter) modifyDB(i *RDSInstance, password string) (base.InstanceState, error) {
	svc := rds.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	// Standard parameters (https://docs.aws.amazon.com/sdk-for-go/api/service/rds/#RDS.ModifyDBInstance)
	// NOTE:  Only instance class modification and Multi AZ (redundancy) is
	// enabled at this point.
	params := &rds.ModifyDBInstanceInput{
		AllocatedStorage:     aws.Int64(i.AllocatedStorage),
		ApplyImmediately:     aws.Bool(true),
		DBInstanceClass:      &d.Plan.InstanceClass,
		MultiAZ:              &d.Plan.Redundant,
		DBInstanceIdentifier: &i.Database,
	}

	resp, err := svc.ModifyDBInstance(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))

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
		svc := rds.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(i.Database),
		}

		resp, err := svc.DescribeDBInstances(params)
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

		// Pretty-print the response data.
		fmt.Println(awsutil.StringValue(resp))

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
		svc := rds.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(i.Database),
			// MaxRecords: aws.Long(1),
		}

		resp, err := svc.DescribeDBInstances(params)
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

		// Pretty-print the response data.
		fmt.Println(awsutil.StringValue(resp))

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

// search out all the parameter groups that we created and try to clean them up
func cleanupCustomParameterGroups(svc *rds.RDS) {
	input := &rds.DescribeDBParameterGroupsInput{}
	err := svc.DescribeDBParameterGroupsPages(input,
		func(pgroups *rds.DescribeDBParameterGroupsOutput, lastPage bool) bool {
			// If the pgroup matches the prefix, then try to delete it.
			// If it's in use, it will fail, so ignore that.
			for _, pgroup := range pgroups.DBParameterGroups {
				matched, err := regexp.Match("^"+PgroupPrefix, []byte(*pgroup.DBParameterGroupName))
				if err != nil {
					log.Printf("error trying to match %s in %s: %s", PgroupPrefix, *pgroup.DBParameterGroupName, err.Error())
				}
				if matched {
					deleteinput := &rds.DeleteDBParameterGroupInput{
						DBParameterGroupName: aws.String(*pgroup.DBParameterGroupName),
					}
					_, err := svc.DeleteDBParameterGroup(deleteinput)
					if err == nil {
						log.Printf("cleaned up %s parameter group", *pgroup.DBParameterGroupName)
					} else {
						// If you can't delete it because it's in use, that is fine.
						// The db takes a while to delete, so we will clean it up the
						// next time this is called.  Otherwise there is some sort of AWS error
						// and we should log that.
						if err.(awserr.Error).Code() != "InvalidDBParameterGroupState" {
							log.Printf("There was an error cleaning up the %s parameter group.  The error was: %s", *pgroup.DBParameterGroupName, err.Error())
						}
					}
				}
			}
			return true
		})
	if err != nil {
		log.Printf("Could not retrieve list of parameter groups while cleaning up: %s", err.Error())
		return
	}
}

func (d *dedicatedDBAdapter) deleteDB(i *RDSInstance) (base.InstanceState, error) {
	svc := rds.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	params := &rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(i.Database), // Required
		// FinalDBSnapshotIdentifier: aws.String("String"),
		SkipFinalSnapshot: aws.Bool(true),
	}
	resp, err := svc.DeleteDBInstance(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		// clean up custom parameter groups
		cleanupCustomParameterGroups(svc)
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
