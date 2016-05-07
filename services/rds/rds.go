package rds

import (
	"github.com/18F/aws-broker/base"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/jinzhu/gorm"

	"errors"
	"fmt"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/response"
	"log"
	"net/http"
)

// DBAdapter contains the method on how to get the right agent depending on the plan.
type DBAdapter interface {
	findBrokerAgent(plan catalog.RDSPlan, c *catalog.Catalog) (DbBrokerAgent, response.Response)
}

// DefaultDBAdapter is the struct to use for normal workflow.
type DefaultDBAdapter struct{}

// DbBrokerAgent is what every type of agent should implement to handle the
// lifecycle of its particular service instances.
type DbBrokerAgent interface {
	createDB(i *Instance, password string) (base.InstanceState, error)
	bindDBToApp(i *Instance, password string) (map[string]string, error)
	deleteDB(i *Instance) (base.InstanceState, error)
}

var (
	// ErrResponseAgentNotFound is an error to describe that the agent is not found or is nil.
	ErrResponseAgentNotFound = response.NewErrorResponse(http.StatusInternalServerError, "DB Broker Agent not found")
	// ErrResponseCatalogNotFound is an error to describe that the catalog could not be found or is nil.
	ErrResponseCatalogNotFound = response.NewErrorResponse(http.StatusInternalServerError, "Catalog not found")
	// ErrResponseRDSSettingsNotFound is an error to describe that the catalog could not be found or is nil.
	ErrResponseRDSSettingsNotFound = response.NewErrorResponse(http.StatusInternalServerError, "RDS Settings not found")
	// ErrResponseDBNotFound is an error to describe that the db connection could not be found or is nil.
	ErrResponseDBNotFound = response.NewErrorResponse(http.StatusInternalServerError, "Shared DB not found")
)

// findBrokerAgent finds which agent to use depending on the plan.
func (a DefaultDBAdapter) findBrokerAgent(plan catalog.RDSPlan, c *catalog.Catalog) (DbBrokerAgent, response.Response) {
	var dbAgent DbBrokerAgent

	switch plan.Agent {
	case "shared":
		if c == nil {
			return nil, ErrResponseCatalogNotFound
		}
		rdsSettings := c.GetResources().RdsSettings
		if rdsSettings == nil {
			return nil, ErrResponseRDSSettingsNotFound
		}
		setting, err := rdsSettings.GetRDSSettingByPlan(plan.ID)
		if err != nil {
			return nil, response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		if setting.DB == nil {
			return nil, ErrResponseDBNotFound
		}
		dbAgent = &sharedAgent{
			SharedDbConn: setting.DB,
		}
	case "dedicated":
		dbAgent = &dedicatedAgent{
			InstanceClass: plan.InstanceClass,
		}
	default:
		return nil, ErrResponseAgentNotFound
	}

	return dbAgent, nil
}

var (
	// ErrInstanceNotFound represents that the instance is null.
	ErrInstanceNotFound = errors.New("Instance not found")
	// ErrIncompleteInstance represents the instance does not have all the necessary details for the operation.
	ErrIncompleteInstance = errors.New("Incomplete instance details")
	// ErrMissingPassword indicates there is an empty password being provided.
	ErrMissingPassword = errors.New("Instance must be secured by password")
	// ErrDatabaseNotFound indicates the database is null.
	ErrDatabaseNotFound = errors.New("Database not found")
	// ErrCannotReachSharedDB indicates the database is unreachable.
	ErrCannotReachSharedDB = errors.New("Unable to reach shared database")
)

type sharedAgent struct {
	SharedDbConn *gorm.DB
}

func isDBConnectionAlive(db *gorm.DB) bool {
	return db.Exec("SELECT 1;").Error == nil
}

func (d *sharedAgent) createDB(i *Instance, password string) (base.InstanceState, error) {
	// Make sure we have a password
	if len(password) < 1 {
		return base.InstanceNotCreated, ErrMissingPassword
	}
	if err := checkSharedInputs(i, d.SharedDbConn); err != nil {
		return base.InstanceNotCreated, err
	}
	switch i.DbType {
	case "postgres":
		// TODO sanitize for reserved postgres words, e.g. "CREATE USER user" would not work
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE DATABASE %s;", i.Database)); db.Error != nil {
			return base.InstanceNotCreated, db.Error
		}
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", i.Username, password)); db.Error != nil {
			// TODO. Revert CREATE DATABASE.
			return base.InstanceNotCreated, db.Error
		}
		if db := d.SharedDbConn.Exec(fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s;", i.Database, i.Username)); db.Error != nil {
			// TODO. Revert CREATE DATABASE and CREATE USER.
			return base.InstanceNotCreated, db.Error
		}
	case "mysql":
		// TODO sanitize for reserved mysql words
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE DATABASE %s;", i.Database)); db.Error != nil {
			return base.InstanceNotCreated, db.Error
		}
		// Double % escapes to one %.
		if db := d.SharedDbConn.Exec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';", i.Username, password)); db.Error != nil {
			// TODO. Revert CREATE DATABASE.
			return base.InstanceNotCreated, db.Error
		}
		// Double % escapes to one %.
		if db := d.SharedDbConn.Exec(fmt.Sprintf("GRANT ALL ON %s.* TO '%s'@'%%';", i.Database, i.Username)); db.Error != nil {
			// TODO. Revert CREATE DATABASE and CREATE USER.
			return base.InstanceNotCreated, db.Error
		}
	default:
		return base.InstanceNotCreated, fmt.Errorf("Unsupported database type: %s, cannot create shared database", i.DbType)
	}
	return base.InstanceReady, nil
}

func (d *sharedAgent) bindDBToApp(i *Instance, password string) (map[string]string, error) {
	return i.getCredentials(password)
}

func (d *sharedAgent) deleteDB(i *Instance) (base.InstanceState, error) {
	// Make sure we have all the details.
	if err := checkSharedInputs(i, d.SharedDbConn); err != nil {
		return base.InstanceNotGone, err
	}
	if db := d.SharedDbConn.Exec(fmt.Sprintf("DROP DATABASE %s;", i.Database)); db.Error != nil {
		return base.InstanceNotGone, db.Error
	}
	if db := d.SharedDbConn.Exec(fmt.Sprintf("DROP USER %s;", i.Username)); db.Error != nil {
		return base.InstanceNotGone, db.Error
	}
	return base.InstanceGone, nil
}

func checkSharedInputs(i *Instance, db *gorm.DB) error {
	// Sanity check for instance
	if i == nil {
		return ErrInstanceNotFound
	}
	// Make sure we have all the details.
	if len(i.Database) < 1 || len(i.Username) < 1 {
		return ErrIncompleteInstance
	}
	// Check database and database connection.
	if db == nil || db.DB() == nil {
		return ErrDatabaseNotFound
	}
	if db.DB().Ping() != nil || !isDBConnectionAlive(db) {
		return ErrCannotReachSharedDB
	}
	return nil
}

type dedicatedAgent struct {
	InstanceClass string
}

func (d *dedicatedAgent) createDB(i *Instance, password string) (base.InstanceState, error) {
	svc := rds.New(session.New(), aws.NewConfig().WithRegion("us-east-1"))
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
		// Everyone gets 10gb for now
		AllocatedStorage: aws.Int64(10),
		// Instance class is defined by the plan
		DBInstanceClass:         &d.InstanceClass,
		DBInstanceIdentifier:    &i.Database,
		DBName:                  &i.Database,
		Engine:                  aws.String(i.DbType),
		MasterUserPassword:      &password,
		MasterUsername:          &i.Username,
		AutoMinorVersionUpgrade: aws.Bool(true),
		MultiAZ:                 aws.Bool(true),
		StorageEncrypted:        aws.Bool(true),
		Tags:                    rdsTags,
		PubliclyAccessible:      aws.Bool(false),
		DBSubnetGroupName:       &i.DbSubnetGroup,
		VpcSecurityGroupIds: []*string{
			&i.SecGroup,
		},
	}

	if *params.DBInstanceClass == "db.t2.micro" {
		params.StorageEncrypted = aws.Bool(false)
	}

	resp, err := svc.CreateDBInstance(params)
	// Pretty-print the response data.
	log.Println(awsutil.StringValue(resp))
	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

func (d *dedicatedAgent) bindDBToApp(i *Instance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		svc := rds.New(session.New(), aws.NewConfig().WithRegion("us-east-1"))
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

func (d *dedicatedAgent) deleteDB(i *Instance) (base.InstanceState, error) {
	svc := rds.New(session.New(), aws.NewConfig().WithRegion("us-east-1"))
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
		return base.InstanceGone, nil
	}
	return base.InstanceNotGone, nil
}

func (d *dedicatedAgent) didAwsCallSucceed(err error) bool {
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
