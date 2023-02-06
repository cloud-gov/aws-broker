package rds

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/18F/aws-broker/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
)

const pgCronLibraryName = "pg_cron"

type parameterGroupAdapterInterface interface {
	ProvisionCustomParameterGroupIfNecessary(i *RDSInstance) error
}

type parameterGroupAdapter struct {
	rds                  rdsiface.RDSAPI
	settings             config.Settings
	parameterGroupPrefix string
}

type paramDetails struct {
	value       string
	applyMethod string
}

func NewParameterGroupAdapter(rds rdsiface.RDSAPI, settings config.Settings) *parameterGroupAdapter {
	return &parameterGroupAdapter{
		rds:                  rds,
		settings:             settings,
		parameterGroupPrefix: "cg-aws-broker-",
	}
}

func (p *parameterGroupAdapter) ProvisionCustomParameterGroupIfNecessary(i *RDSInstance) error {
	if !p.needCustomParameters(i) {
		return nil
	}

	p.setParameterGroupName(i)

	customRDSParameters, err := p.getCustomParameters(i)
	if err != nil {
		return fmt.Errorf("encountered error getting custom parameters: %w", err)
	}

	// apply parameter group
	err = p.createOrModifyCustomParameterGroup(i, customRDSParameters)
	if err != nil {
		log.Println(err.Error())
		return fmt.Errorf("encountered error applying parameter group: %w", err)
	}
	return nil
}

// CleanupCustomParameterGroups searches out all the parameter groups that we created and tries to clean them up
func (p *parameterGroupAdapter) CleanupCustomParameterGroups() {
	input := &rds.DescribeDBParameterGroupsInput{}
	err := p.rds.DescribeDBParameterGroupsPages(input, func(pgroups *rds.DescribeDBParameterGroupsOutput, lastPage bool) bool {
		// If the pgroup matches the prefix, then try to delete it.
		// If it's in use, it will fail, so ignore that.
		for _, pgroup := range pgroups.DBParameterGroups {
			matched, err := regexp.Match("^"+p.parameterGroupPrefix, []byte(*pgroup.DBParameterGroupName))
			if err != nil {
				log.Printf("error trying to match %s in %s: %s", p.parameterGroupPrefix, *pgroup.DBParameterGroupName, err.Error())
			}
			if matched {
				deleteinput := &rds.DeleteDBParameterGroupInput{
					DBParameterGroupName: aws.String(*pgroup.DBParameterGroupName),
				}
				_, err := p.rds.DeleteDBParameterGroup(deleteinput)
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
	}
}

func (p *parameterGroupAdapter) getParameterGroupFamily(i *RDSInstance) error {
	if i.ParameterGroupFamily != "" {
		return nil
	}
	parameterGroupFamily := ""
	// If the DB version is not set (e.g., creating a new instance without
	// providing a specific version), determine the default parameter group
	// name from the default engine that will be chosen.
	if i.DbVersion == "" {
		dbEngineVersionsInput := &rds.DescribeDBEngineVersionsInput{
			DefaultOnly: aws.Bool(true),
			Engine:      aws.String(i.DbType),
		}

		// This call requires that the broker have permissions to make it.
		defaultEngineInfo, err := p.rds.DescribeDBEngineVersions(dbEngineVersionsInput)
		if err != nil {
			return err
		}

		// The value from the engine info is a string pointer, so we must
		// retrieve its actual value.
		parameterGroupFamily = *defaultEngineInfo.DBEngineVersions[0].DBParameterGroupFamily
	} else {
		// The DB instance has a version, therefore we can derive the
		// parameter group family directly.
		re := regexp.MustCompile(`^\d+\.*\d*`)
		dbversion := re.Find([]byte(i.DbVersion))
		parameterGroupFamily = i.DbType + string(dbversion)
	}
	log.Printf("got parameter group family: %s", parameterGroupFamily)
	i.ParameterGroupFamily = parameterGroupFamily
	return nil
}

func (p *parameterGroupAdapter) checkIfParameterGroupExists(i *RDSInstance) bool {
	dbParametersInput := &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(i.ParameterGroupName),
		MaxRecords:           aws.Int64(20),
		Source:               aws.String("system"),
	}

	// If the db parameter group has already been created, we can return.
	_, err := p.rds.DescribeDBParameters(dbParametersInput)
	parameterGroupExists := (err == nil)
	if parameterGroupExists {
		log.Printf("%s parameter group already exists", i.ParameterGroupName)
	}
	return parameterGroupExists
}

func (p *parameterGroupAdapter) getParameterGroupName(i *RDSInstance) string {
	// i.FormatDBName() should always return the same value for the same database name,
	// so the parameter group name should remain consistent
	return p.parameterGroupPrefix + i.FormatDBName()
}

func (p *parameterGroupAdapter) setParameterGroupName(i *RDSInstance) {
	if i.ParameterGroupName != "" {
		return
	}
	i.ParameterGroupName = p.getParameterGroupName(i)
}

// This function will return the a custom parameter group with whatever custom
// parameters have been requested.  If there is no custom parameter group, it
// will be created.
func (p *parameterGroupAdapter) createOrModifyCustomParameterGroup(
	i *RDSInstance,
	customparams map[string]map[string]paramDetails,
) error {
	parameterGroupExists := p.checkIfParameterGroupExists(i)
	if !parameterGroupExists {
		// Otherwise, create a new parameter group in the proper family.
		err := p.getParameterGroupFamily(i)
		if err != nil {
			return fmt.Errorf("encounted error getting parameter group family: %w", err)
		}

		log.Printf("creating a parameter group named %s in the family of %s", i.ParameterGroupName, i.ParameterGroupFamily)
		createInput := &rds.CreateDBParameterGroupInput{
			DBParameterGroupFamily: aws.String(i.ParameterGroupFamily),
			DBParameterGroupName:   aws.String(i.ParameterGroupName),
			Description:            aws.String("aws broker parameter group for " + i.FormatDBName()),
		}

		_, err = p.rds.CreateDBParameterGroup(createInput)
		if err != nil {
			return fmt.Errorf("encountered error when creating parameter group: %w", err)
		}
	}

	// iterate through the options and plug them into the parameter list
	parameters := []*rds.Parameter{}
	for paramName, paramDetails := range customparams[i.DbType] {
		parameters = append(parameters, &rds.Parameter{
			ApplyMethod:    aws.String(paramDetails.applyMethod),
			ParameterName:  aws.String(paramName),
			ParameterValue: aws.String(paramDetails.value),
		})
	}

	// modify the parameter group we just created with the parameter list
	modifyinput := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(i.ParameterGroupName),
		Parameters:           parameters,
	}
	_, err := p.rds.ModifyDBParameterGroup(modifyinput)
	if err != nil {
		return err
	}

	return nil
}

func (p *parameterGroupAdapter) needCustomParameters(i *RDSInstance) bool {
	if i.EnableFunctions &&
		p.settings.EnableFunctionsFeature &&
		(i.DbType == "mysql") {
		return true
	}
	if i.BinaryLogFormat != "" &&
		(i.DbType == "mysql") {
		return true
	}
	if (i.EnablePgCron || i.DisablePgCron) &&
		(i.DbType == "postgres") {
		return true
	}

	return false
}

func (p *parameterGroupAdapter) getDefaultEngineParameterValue(paramName string, i *RDSInstance) (string, error) {
	err := p.getParameterGroupFamily(i)
	if err != nil {
		return "", err
	}

	describeEngDefaultParamsInput := &rds.DescribeEngineDefaultParametersInput{
		DBParameterGroupFamily: &i.ParameterGroupFamily,
	}
	more := true
	for more {
		result, err := p.rds.DescribeEngineDefaultParameters(describeEngDefaultParamsInput)
		if err != nil {
			return "", err
		}
		for _, param := range result.EngineDefaults.Parameters {
			if *param.ParameterName == paramName {
				log.Printf("found default parameter value %s for parameter %s", *param.ParameterValue, *param.ParameterName)
				return *param.ParameterValue, nil
			}
		}
		describeEngDefaultParamsInput.Marker = result.EngineDefaults.Marker
		more = describeEngDefaultParamsInput.Marker != nil && *describeEngDefaultParamsInput.Marker != ""
	}
	return "", nil
}

func (p *parameterGroupAdapter) buildCustomSharePreloadLibrariesParam(
	i *RDSInstance,
	customLibrary string,
) (string, error) {
	defaultSharedPreloadLibraries, err := p.getDefaultEngineParameterValue("shared_preload_libraries", i)
	if err != nil {
		return "", err
	}
	libraries := []string{
		customLibrary,
	}
	if defaultSharedPreloadLibraries != "" {
		libraries = append(libraries, defaultSharedPreloadLibraries)
	}
	customSharePreloadLibrariesParam := strings.Join(libraries, ",")
	log.Printf("generated custom share_preload_libraries param: %s", customSharePreloadLibrariesParam)
	return customSharePreloadLibrariesParam, nil
}

func (p *parameterGroupAdapter) findParameterValueInResults(
	i *RDSInstance,
	parameters []*rds.Parameter,
	parameterName string,
) bool {
	if i.ParameterValues == nil {
		i.ParameterValues = make(map[string]string)
	}
	for _, param := range parameters {
		if *param.ParameterName == parameterName {
			log.Printf("found parameter value %s for parameter %s", *param.ParameterValue, parameterName)
			i.ParameterValues[parameterName] = *param.ParameterValue
		}
	}
	return i.ParameterValues[parameterName] == ""
}

func (p *parameterGroupAdapter) getCustomParameterValue(i *RDSInstance, parameterName string) (string, error) {
	err := p.getParameterGroupFamily(i)
	if err != nil {
		return "", err
	}
	dbParametersInput := &rds.DescribeDBParametersInput{
		DBParameterGroupName: aws.String(i.ParameterGroupName),
		Source:               aws.String("user"),
	}
	err = p.rds.DescribeDBParametersPages(dbParametersInput, func(result *rds.DescribeDBParametersOutput, lastPage bool) bool {
		return p.findParameterValueInResults(i, result.Parameters, parameterName)
	})
	if err != nil {
		return "", err
	}
	return i.ParameterValues[parameterName], nil
}

func (p *parameterGroupAdapter) getCustomParameters(i *RDSInstance) (map[string]map[string]paramDetails, error) {
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
		if i.EnablePgCron {
			preloadLibrariesParam, err := p.buildCustomSharePreloadLibrariesParam(i, pgCronLibraryName)
			if err != nil {
				return nil, err
			}
			customRDSParameters["postgres"]["shared_preload_libraries"] = paramDetails{
				value:       preloadLibrariesParam,
				applyMethod: "pending-reboot",
			}
		}
	}

	return customRDSParameters, nil
}
