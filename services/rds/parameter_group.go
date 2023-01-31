package rds

import (
	"log"
	"regexp"

	"github.com/18F/aws-broker/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
)

// PgroupPrefix is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-aws-broker-"

var (
	needCustomParameters               = needCustomParametersFunc
	getCustomParameters                = getCustomParametersFunc
	createOrModifyCustomParameterGroup = createOrModifyCustomParameterGroupFunc
)

type parameterGroupAdapterInterface interface {
	provisionCustomParameterGroupIfNecessary(
		i *RDSInstance,
		d *dedicatedDBAdapter,
		svc rdsiface.RDSAPI,
	) (string, error)
}

type parameterGroupAdapter struct{}

func getParameterGroupFamily(
	i *RDSInstance,
	svc rdsiface.RDSAPI,
) (string, error) {
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
			return "", err
		}

		// The value from the engine info is a string pointer, so we must
		// retrieve its actual value.
		pgroupFamily := *defaultEngineInfo.DBEngineVersions[0].DBParameterGroupFamily
		return pgroupFamily, nil
	}

	// The DB instance has a version, therefore we can derive the
	// parameter group family directly.
	re := regexp.MustCompile(`^\d+\.*\d*`)
	dbversion := re.Find([]byte(i.DbVersion))
	pgroupFamily := i.DbType + string(dbversion)
	return pgroupFamily, nil
}

// This function will return the a custom parameter group with whatever custom
// parameters have been requested.  If there is no custom parameter group, it
// will be created.
func createOrModifyCustomParameterGroupFunc(
	i *RDSInstance,
	customparams map[string]map[string]string,
	svc rdsiface.RDSAPI,
) (string, error) {
	// i.FormatDBName() should always return the same value for the same database name,
	// so the parameter group name should remain consistent
	pgroupName := PgroupPrefix + i.FormatDBName()

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
		}

		log.Printf("creating a parameter group named %s in the family of %s", pgroupName, pgroupFamily)
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
func needCustomParametersFunc(i *RDSInstance, s config.Settings) bool {
	// Currently, we only have one custom parameter for mysql, but if
	// we ever need to apply more, you can add them in here.
	if i.EnableFunctions &&
		s.EnableFunctionsFeature &&
		(i.DbType == "mysql") {
		return true
	}
	if i.BinaryLogFormat != "" &&
		(i.DbType == "mysql") {
		return true
	}
	return false
}

func getCustomParametersFunc(i *RDSInstance, s config.Settings) map[string]map[string]string {
	customRDSParameters := make(map[string]map[string]string)

	// enable functions
	customRDSParameters["mysql"] = make(map[string]string)
	if i.EnableFunctions && s.EnableFunctionsFeature {
		customRDSParameters["mysql"]["log_bin_trust_function_creators"] = "1"
	} else {
		customRDSParameters["mysql"]["log_bin_trust_function_creators"] = "0"
	}

	// set MySQL binary log format
	if i.BinaryLogFormat != "" {
		customRDSParameters["mysql"]["binlog_format"] = i.BinaryLogFormat
	}

	// If you need to add more custom parameters, you can add them in here.

	return customRDSParameters
}

func (p *parameterGroupAdapter) provisionCustomParameterGroupIfNecessary(
	i *RDSInstance,
	d *dedicatedDBAdapter,
	svc rdsiface.RDSAPI,
) (string, error) {
	if !needCustomParameters(i, d.settings) {
		return "", nil
	}

	customRDSParameters := getCustomParameters(i, d.settings)

	// apply parameter group
	pgroupName, err := createOrModifyCustomParameterGroup(i, customRDSParameters, svc)
	if err != nil {
		log.Println(err.Error())
		return "", err
	}
	return pgroupName, nil
}

// search out all the parameter groups that we created and try to clean them up
func cleanupCustomParameterGroups(svc rdsiface.RDSAPI) {
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
					} else if err.(awserr.Error).Code() != "InvalidDBParameterGroupState" {
						// If you can't delete it because it's in use, that is fine.
						// The db takes a while to delete, so we will clean it up the
						// next time this is called.  Otherwise there is some sort of AWS error
						// and we should log that.
						log.Printf("There was an error cleaning up the %s parameter group.  The error was: %s", *pgroup.DBParameterGroupName, err.Error())
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
