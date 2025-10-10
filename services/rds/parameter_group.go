package rds

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/config"
)

const pgCronLibraryName = "pg_cron"
const sharedPreloadLibrariesParameterName = "shared_preload_libraries"

type parameterGroupClient interface {
	ProvisionCustomParameterGroupIfNecessary(i *RDSInstance, rdsTags []rdsTypes.Tag) error
	CleanupCustomParameterGroups() error
}

// awsParameterGroupClient provides abstractions for calls to the AWS RDS API for parameter groups
type awsParameterGroupClient struct {
	rds                  RDSClientInterface
	settings             config.Settings
	parameterGroupPrefix string
}

type paramDetails struct {
	value       string
	applyMethod string
}

func NewAwsParameterGroupClient(rds RDSClientInterface, settings config.Settings) *awsParameterGroupClient {
	return &awsParameterGroupClient{
		rds:                  rds,
		settings:             settings,
		parameterGroupPrefix: "cg-aws-broker-",
	}
}

// ProvisionCustomParameterGroupIfNecessary determines from the RDS instance struct whether
// there needs to be a custom parameter group for the instance. If so, the method will either
// create a new parameter group or modify an existing one with the correct parameters for the
// instance
func (p *awsParameterGroupClient) ProvisionCustomParameterGroupIfNecessary(i *RDSInstance, rdsTags []rdsTypes.Tag) error {
	if !p.needCustomParameters(i) {
		return nil
	}

	customRDSParameters, err := p.getCustomParameters(i)
	if err != nil {
		return fmt.Errorf("encountered error getting custom parameters: %w", err)
	}

	setParameterGroupName(i, p)

	// apply parameter group
	err = p.createOrModifyCustomParameterGroup(i, rdsTags, customRDSParameters)
	if err != nil {
		log.Println(err.Error())
		return fmt.Errorf("encountered error applying parameter group: %w", err)
	}
	return nil
}

// CleanupCustomParameterGroups searches out all the parameter groups that we created and tries to clean them up
func (p *awsParameterGroupClient) CleanupCustomParameterGroups() error {
	input := &rds.DescribeDBParameterGroupsInput{}
	paginator := rds.NewDescribeDBParameterGroupsPaginator(p.rds, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			return fmt.Errorf("CleanupCustomParameterGroups: error handling next page: %w", err)
		}

		// If the pgroup matches the prefix, then try to delete it.
		// If it's in use, it will fail, so ignore that.
		for _, pgroup := range output.DBParameterGroups {
			matched, err := regexp.Match("^"+p.parameterGroupPrefix, []byte(*pgroup.DBParameterGroupName))
			if err != nil {
				log.Printf("error trying to match %s in %s: %s", p.parameterGroupPrefix, *pgroup.DBParameterGroupName, err.Error())
			}
			if matched {
				deleteinput := &rds.DeleteDBParameterGroupInput{
					DBParameterGroupName: aws.String(*pgroup.DBParameterGroupName),
				}
				_, err := p.rds.DeleteDBParameterGroup(context.TODO(), deleteinput)
				if err != nil {
					var exception *rdsTypes.InvalidDBParameterGroupStateFault
					if errors.As(err, &exception) {
						// If you can't delete it because it's in use, that is fine.
						// The db takes a while to delete, so we will clean it up the
						// next time this is called.  Otherwise there is some sort of AWS error
						// and we should log that.
						log.Printf("There was an error cleaning up the %s parameter group.  The error was: %s", *pgroup.DBParameterGroupName, err.Error())
						continue
					}

					return fmt.Errorf("CleanupCustomParameterGroups: DeleteDBParameterGroup err %w", err)
				}

				log.Printf("cleaned up %s parameter group", *pgroup.DBParameterGroupName)
			}
		}
	}

	return nil
}

func (p *awsParameterGroupClient) getDatabaseEngineVersion(i *RDSInstance) (string, error) {
	if i.Database == "" {
		return "", errors.New("database name is required to get database engine version")
	}

	dbInstanceInfo, err := p.rds.DescribeDBInstances(context.TODO(), &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(i.Database),
	})
	if err != nil {
		return "", err
	}

	// Take the engine version of the database
	dbVersion := *dbInstanceInfo.DBInstances[0].EngineVersion
	if dbVersion == "" {
		return "", errors.New("could not determine DB version to set parameter group family")
	}

	return dbVersion, nil
}

func (p *awsParameterGroupClient) getParameterGroupFamily(i *RDSInstance) error {
	if i.ParameterGroupFamily != "" {
		return nil
	}
	parameterGroupFamily := ""

	if i.DbVersion == "" {
		dbVersion, err := p.getDatabaseEngineVersion(i)
		if err != nil {
			return err
		}
		i.DbVersion = dbVersion
	}

	dbEngineVersionsInput := &rds.DescribeDBEngineVersionsInput{
		Engine:        aws.String(i.DbType),
		EngineVersion: aws.String(i.DbVersion),
	}

	// This call requires that the broker have permissions to make it.
	defaultEngineInfo, err := p.rds.DescribeDBEngineVersions(context.TODO(), dbEngineVersionsInput)
	if err != nil {
		return err
	}

	// The value from the engine info is a string pointer, so we must
	// retrieve its actual value.
	parameterGroupFamily = *defaultEngineInfo.DBEngineVersions[0].DBParameterGroupFamily

	log.Printf("got parameter group family: %s", parameterGroupFamily)
	i.ParameterGroupFamily = parameterGroupFamily
	return nil
}

func (p *awsParameterGroupClient) checkIfParameterGroupExists(parameterGroupName string) (bool, error) {
	dbParametersInput := &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(parameterGroupName),
		MaxRecords:           aws.Int32(20),
		Source:               aws.String("system"),
	}

	// If the db parameter group has already been created, we can return.
	_, err := p.rds.DescribeDBParameters(context.TODO(), dbParametersInput)
	if err == nil {
		log.Printf("%s parameter group already exists", parameterGroupName)
		return true, nil
	}

	var notFoundException *rdsTypes.DBParameterGroupNotFoundFault
	if errors.As(err, &notFoundException) {
		return false, nil
	}

	return false, err
}

// This function will either modify or create a custom parameter group with whatever custom
// parameters have been requested.
func (p *awsParameterGroupClient) createOrModifyCustomParameterGroup(
	i *RDSInstance,
	rdsTags []rdsTypes.Tag,
	customparams map[string]map[string]paramDetails,
) error {
	parameterGroupExists, err := p.checkIfParameterGroupExists(i.ParameterGroupName)
	if err != nil {
		return fmt.Errorf("createOrModifyCustomParameterGroup: checkIfParameterGroupExists err %w", err)
	}
	if !parameterGroupExists {
		// Otherwise, create a new parameter group in the proper family.
		err := p.getParameterGroupFamily(i)
		if err != nil {
			return fmt.Errorf("createOrModifyCustomParameterGroup: encountered error getting parameter group family: %w", err)
		}

		log.Printf("creating a parameter group named %s in the family of %s", i.ParameterGroupName, i.ParameterGroupFamily)
		createInput := &rds.CreateDBParameterGroupInput{
			DBParameterGroupFamily: aws.String(i.ParameterGroupFamily),
			DBParameterGroupName:   aws.String(i.ParameterGroupName),
			Description:            aws.String("aws broker parameter group for " + i.FormatDBName()),
			Tags:                   rdsTags,
		}

		_, err = p.rds.CreateDBParameterGroup(context.TODO(), createInput)
		if err != nil {
			return fmt.Errorf("createOrModifyCustomParameterGroup: encountered error when creating parameter group: %w", err)
		}
	}

	// iterate through the options and plug them into the parameter list
	parameters := []rdsTypes.Parameter{}
	for paramName, paramDetails := range customparams[i.DbType] {
		applyMethod, err := getRdsApplyMethodEnum(paramDetails.applyMethod)
		if err != nil {
			return fmt.Errorf("createOrModifyCustomParameterGroup: error getting apply method: %s", err)
		}

		parameters = append(parameters, rdsTypes.Parameter{
			ApplyMethod:    *applyMethod,
			ParameterName:  aws.String(paramName),
			ParameterValue: aws.String(paramDetails.value),
		})
	}

	// modify the parameter group we just created with the parameter list
	modifyinput := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(i.ParameterGroupName),
		Parameters:           parameters,
	}

	_, err = p.rds.ModifyDBParameterGroup(context.TODO(), modifyinput)
	if err != nil {
		return err
	}

	return nil
}

func (p *awsParameterGroupClient) needCustomParameters(i *RDSInstance) bool {
	if i.EnableFunctions &&
		p.settings.EnableFunctionsFeature &&
		(i.DbType == "mysql") {
		return true
	}
	if i.BinaryLogFormat != "" &&
		(i.DbType == "mysql") {
		return true
	}
	if i.EnablePgCron != nil &&
		(i.DbType == "postgres") {
		return true
	}

	return false
}

func (p *awsParameterGroupClient) getDefaultEngineParameterValue(i *RDSInstance, parameterName string) (string, error) {
	err := p.getParameterGroupFamily(i)
	if err != nil {
		return "", fmt.Errorf("getDefaultEngineParameterValue: getParameterGroupFamily err: %w", err)
	}
	describeEngDefaultParamsInput := &rds.DescribeEngineDefaultParametersInput{
		DBParameterGroupFamily: &i.ParameterGroupFamily,
	}

	paginator := rds.NewDescribeEngineDefaultParametersPaginator(p.rds, describeEngDefaultParamsInput)
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(context.TODO())
		if err != nil {
			return "", fmt.Errorf("getDefaultEngineParameterValue: error handling next page: %w", err)
		}
		foundValue := findParameterValueInResults(result.EngineDefaults.Parameters, parameterName)
		if foundValue != "" {
			return foundValue, nil
		}
	}

	return "", nil
}

func (p *awsParameterGroupClient) getCustomParameterValue(i *RDSInstance, parameterName string) (string, error) {
	dbParametersInput := &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(i.ParameterGroupName),
		Source:               aws.String("user"),
	}
	// We have to use a channel to get the parameter value from the anonymous function to DescribeDBParametersPages
	// because the code is executed asychronously
	paginator := rds.NewDescribeDBParametersPaginator(p.rds, dbParametersInput)
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(context.TODO())
		if err != nil {
			return "", fmt.Errorf("getCustomParameterValue: error handling next page: %w", err)
		}
		foundValue := findParameterValueInResults(result.Parameters, parameterName)
		if foundValue != "" {
			return foundValue, nil
		}
	}

	return "", nil
}

// getParameterValue will get the value of a parameter from the instance's custom parameter group if one exists
// or from the engine default parameter group if not
func (p *awsParameterGroupClient) getParameterValue(i *RDSInstance, parameterName string) (string, error) {
	if i.ParameterGroupName != "" {
		log.Printf("getting parameter %s from group %s", parameterName, i.ParameterGroupName)
		return p.getCustomParameterValue(i, parameterName)
	}
	log.Printf("getting parameter %s from engine defaults", parameterName)
	return p.getDefaultEngineParameterValue(i, parameterName)
}

func (p *awsParameterGroupClient) getCustomParameters(i *RDSInstance) (map[string]map[string]paramDetails, error) {
	customRDSParameters := make(map[string]map[string]paramDetails)

	if i.DbType == "mysql" {
		// enable functions
		customRDSParameters["mysql"] = make(map[string]paramDetails)
		if i.EnableFunctions && p.settings.EnableFunctionsFeature {
			customRDSParameters["mysql"]["log_bin_trust_function_creators"] = paramDetails{
				value:       "1",
				applyMethod: "immediate",
			}

		} else {
			customRDSParameters["mysql"]["log_bin_trust_function_creators"] = paramDetails{
				value:       "0",
				applyMethod: "immediate",
			}
		}

		// set MySQL binary log format
		if i.BinaryLogFormat != "" {
			customRDSParameters["mysql"]["binlog_format"] = paramDetails{
				value:       i.BinaryLogFormat,
				applyMethod: "immediate",
			}
		}
	}

	if i.DbType == "postgres" {
		customRDSParameters["postgres"] = make(map[string]paramDetails)
		if i.EnablePgCron != nil {
			parameterValue, err := p.getParameterValue(i, sharedPreloadLibrariesParameterName)
			if err != nil {
				return nil, err
			}
			var sharedPreloadLibsParamValue string
			if *i.EnablePgCron {
				sharedPreloadLibsParamValue = addLibraryToSharedPreloadLibraries(parameterValue, pgCronLibraryName)
			} else {
				sharedPreloadLibsParamValue = removeLibraryFromSharedPreloadLibraries(parameterValue, pgCronLibraryName)
			}
			customRDSParameters["postgres"][sharedPreloadLibrariesParameterName] = paramDetails{
				value:       sharedPreloadLibsParamValue,
				applyMethod: "pending-reboot",
			}
		}
	}

	return customRDSParameters, nil
}

// getParameterGroupName gets a parameter group name for the instance
func getParameterGroupName(i *RDSInstance, p *awsParameterGroupClient) string {
	// i.FormatDBName() should always return the same value for the same database name,
	// so the parameter group name should remain consistent
	return p.parameterGroupPrefix + i.FormatDBName()
}

// setParameterGroupName sets the parameter group name on the instance struct
func setParameterGroupName(i *RDSInstance, p *awsParameterGroupClient) {
	if i.ParameterGroupName != "" {
		return
	}
	i.ParameterGroupName = getParameterGroupName(i, p)
}

// findParameterValueInResults finds the parameter value in a set of parameters, if any
func findParameterValueInResults(
	parameters []rdsTypes.Parameter,
	parameterName string,
) string {
	var parameterValue string
	for _, param := range parameters {
		if *param.ParameterName == parameterName {
			log.Printf("found parameter value %s for parameter %s", *param.ParameterValue, parameterName)
			parameterValue = *param.ParameterValue
		}
	}
	return parameterValue
}

// addLibraryToSharedPreloadLibraries adds the specified custom library name to the current value of the shared_preload_libraries parameter.
func addLibraryToSharedPreloadLibraries(
	currentParameterValue string,
	customLibrary string,
) string {
	libraries := []string{}
	if customLibrary != "" && !strings.Contains(currentParameterValue, customLibrary) {
		libraries = append(libraries, customLibrary)
	}
	if currentParameterValue != "" {
		libraries = append(libraries, currentParameterValue)
	}
	slices.Sort(libraries)
	customSharePreloadLibrariesParam := strings.Join(libraries, ",")
	log.Printf("generated custom %s param: %s", sharedPreloadLibrariesParameterName, customSharePreloadLibrariesParam)
	return customSharePreloadLibrariesParam
}

// removeLibraryFromSharedPreloadLibraries removes the specified custom library name from the current value of the shared_preload_libraries parameter.
func removeLibraryFromSharedPreloadLibraries(
	currentParameterValue,
	customLibrary string,
) string {
	if currentParameterValue == "" {
		log.Printf("Parameter value for %s is required\n, none found", sharedPreloadLibrariesParameterName)
		return currentParameterValue
	}
	libraries := strings.Split(currentParameterValue, ",")
	for idx, library := range libraries {
		if library == customLibrary {
			libraries = append(libraries[:idx], libraries[idx+1:]...)
		}
	}
	customSharePreloadLibrariesParam := strings.Join(libraries, ",")
	log.Printf("generated custom %s param: %s", sharedPreloadLibrariesParameterName, customSharePreloadLibrariesParam)
	return customSharePreloadLibrariesParam
}
