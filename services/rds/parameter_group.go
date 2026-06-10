package rds

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/lib/pq"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/config"
)

const pgCronLibraryName = "pg_cron"
const sharedPreloadLibrariesParameterName = "shared_preload_libraries"

type parameterGroupClient interface {
	ProvisionNewCustomParameterGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) error
	ProvisionOrModifyCustomParameterGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) (bool, error)
	CleanupCustomParameterGroups() error
	DeleteParameterGroup(parameterGroupName string) error
	IsCustomParameterGroup(parameterGroupName string) bool
	ReconcileRDSInstanceParameterGroup(dbInstanceState *rdsTypes.DBInstance, i RDSInstance) (*RDSInstance, error)
}

// awsParameterGroupClient provides abstractions for calls to the AWS RDS API for parameter groups
type awsParameterGroupClient struct {
	ctx                  context.Context
	rds                  RDSClientInterface
	settings             *config.Settings
	parameterGroupPrefix string
	logger               *slog.Logger
}

type paramDetails struct {
	value       string
	applyMethod string
}

func NewAwsParameterGroupClient(ctx context.Context, rds RDSClientInterface, settings *config.Settings, logger *slog.Logger) *awsParameterGroupClient {
	return &awsParameterGroupClient{
		ctx:                  ctx,
		rds:                  rds,
		settings:             settings,
		parameterGroupPrefix: "cg-aws-broker-",
		logger:               logger,
	}
}

func (p *awsParameterGroupClient) IsCustomParameterGroup(parameterGroupName string) bool {
	return strings.HasPrefix(parameterGroupName, p.parameterGroupPrefix)
}

func (p *awsParameterGroupClient) ProvisionNewCustomParameterGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) error {
	if !p.needCustomParameters(i) {
		return nil
	}

	newRDSParameters, err := p.getNewParameters(i)

	setParameterGroupName(i, p)

	err = p.createOrModifyCustomParameterGroup(i, rdsTags, newRDSParameters, true)
	if err != nil {
		log.Println(err.Error())
		return fmt.Errorf("encountered error applying parameter group: %w", err)
	}
	return nil
}

// ProvisionOrModifyCustomParameterGroup determines from the RDS instance struct whether
// there needs to be a custom parameter group for the instance. If so, the method will either
// create a new parameter group or modify an existing one with the correct parameters for the
// instance
func (p *awsParameterGroupClient) ProvisionOrModifyCustomParameterGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) (bool, error) {
	if !p.needCustomParameters(i) {
		return false, nil
	}

	var parameterGroupExists bool
	var err error

	existingParameterGroupName := i.ParameterGroupName
	if existingParameterGroupName != "" {
		parameterGroupExists, err = p.checkIfParameterGroupExists(existingParameterGroupName)
		if err != nil {
			return false, fmt.Errorf("checkIfParameterGroupExists err %w", err)
		}
	}

	// if a parameter group with the current name already exists, include its parameters
	// in the parameters to be set
	customRDSParameters, err := p.getAllCustomParameters(i, parameterGroupExists)

	// set the parameter group name, which may or may not change depending on if there is a
	// new database version
	setParameterGroupName(i, p)

	// check if a parameter group exists for the updated name
	parameterGroupExists, err = p.checkIfParameterGroupExists(i.ParameterGroupName)
	if err != nil {
		return false, fmt.Errorf("checkIfParameterGroupExists err %w", err)
	}
	shouldCreateParameterGroup := !parameterGroupExists && i.ParameterGroupName != existingParameterGroupName

	err = p.createOrModifyCustomParameterGroup(i, rdsTags, customRDSParameters, shouldCreateParameterGroup)
	if err != nil {
		return false, fmt.Errorf("encountered error applying parameter group: %w", err)
	}

	return shouldCreateParameterGroup, nil
}

// CleanupCustomParameterGroups searches out all the parameter groups that we created and tries to clean them up
func (p *awsParameterGroupClient) CleanupCustomParameterGroups() error {
	input := &rds.DescribeDBParameterGroupsInput{}
	paginator := rds.NewDescribeDBParameterGroupsPaginator(p.rds, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(p.ctx)
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
				_, err := p.rds.DeleteDBParameterGroup(p.ctx, deleteinput)
				if err != nil {
					var invalidParameterGroupStateErr *rdsTypes.InvalidDBParameterGroupStateFault
					if errors.As(err, &invalidParameterGroupStateErr) {
						// If you can't delete it because it's in use, that is fine.
						// The db takes a while to delete, so we will clean it up the
						// next time this is called.
						p.logger.Debug(fmt.Sprintf("There was an error cleaning up the %s parameter group. The error was: %s", *pgroup.DBParameterGroupName, err))
						continue
					}

					var notFoundErr *rdsTypes.DBParameterGroupNotFoundFault
					if errors.As(err, &notFoundErr) {
						p.logger.Debug(fmt.Sprintf("parameter group %s was not found, continuing", *pgroup.DBParameterGroupName))
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

	dbInstanceInfo, err := p.rds.DescribeDBInstances(p.ctx, &rds.DescribeDBInstancesInput{
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
	defaultEngineInfo, err := p.rds.DescribeDBEngineVersions(p.ctx, dbEngineVersionsInput)
	if err != nil {
		return err
	}

	// The value from the engine info is a string pointer, so we must
	// retrieve its actual value.
	parameterGroupFamily = *defaultEngineInfo.DBEngineVersions[0].DBParameterGroupFamily

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
	_, err := p.rds.DescribeDBParameters(p.ctx, dbParametersInput)
	if err == nil {
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
	shouldCreateParameterGroup bool,
) error {
	if shouldCreateParameterGroup {
		// Otherwise, create a new parameter group in the proper family.
		err := p.getParameterGroupFamily(i)
		if err != nil {
			return fmt.Errorf("createOrModifyCustomParameterGroup: encountered error getting parameter group family: %w", err)
		}

		log.Printf("creating a parameter group named %s in the family of %s", i.ParameterGroupName, i.ParameterGroupFamily)
		createInput := &rds.CreateDBParameterGroupInput{
			DBParameterGroupFamily: aws.String(i.ParameterGroupFamily),
			DBParameterGroupName:   aws.String(i.ParameterGroupName),
			Description:            aws.String("aws broker parameter group for " + formatDBName(i.Database)),
			Tags:                   rdsTags,
		}

		_, err = p.rds.CreateDBParameterGroup(p.ctx, createInput)
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

	_, err := p.rds.ModifyDBParameterGroup(p.ctx, modifyinput)
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
	if i.DbType == "postgres" &&
		(i.EnablePgCron != nil || i.PgQueryLogging != nil) {
		return true
	}
	if i.DbType == "mysql" &&
		(slices.Contains(i.EnabledCloudwatchLogGroupExports, "general") ||
			slices.Contains(i.EnabledCloudwatchLogGroupExports, "slowquery")) {
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
		result, err := paginator.NextPage(p.ctx)
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
	paginator := rds.NewDescribeDBParametersPaginator(p.rds, dbParametersInput)
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(p.ctx)
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

func (p *awsParameterGroupClient) getAllCustomParameters(i *RDSInstance, parameterGroupExists bool) (map[string]map[string]paramDetails, error) {
	var existingRDSParameters map[string]map[string]paramDetails
	var err error

	existingRDSParameters = make(map[string]map[string]paramDetails)
	customRDSParameters := make(map[string]map[string]paramDetails)

	if parameterGroupExists {
		existingRDSParameters, err = p.getExistingParameters(i)
		if err != nil {
			return customRDSParameters, err
		}
	}

	newRDSParameters, err := p.getNewParameters(i)
	if err != nil {
		return customRDSParameters, err
	}

	customRDSParameters = newRDSParameters

	// combine existing parameters with any new parameters being set
	for dbType, dbParams := range existingRDSParameters {
		for paramName, paramDetails := range dbParams {
			if _, ok := customRDSParameters[dbType]; ok {
				// only add existing parameter if it is not being customized
				if _, ok := customRDSParameters[dbType][paramName]; !ok {
					customRDSParameters[dbType][paramName] = paramDetails
				}
			}
		}
	}

	return customRDSParameters, nil
}

func (p *awsParameterGroupClient) getExistingParameters(i *RDSInstance) (map[string]map[string]paramDetails, error) {
	existingRDSParameters := make(map[string]map[string]paramDetails)
	output, err := p.rds.DescribeDBParameters(p.ctx, &rds.DescribeDBParametersInput{
		DBParameterGroupName: &i.ParameterGroupName,
		// only need to copy parameters that were modified by the broker or manually
		Source: aws.String("user"),
	})
	if err != nil {
		return existingRDSParameters, fmt.Errorf("encountered error describing parameter group: %w", err)
	}
	for _, param := range output.Parameters {
		if existingRDSParameters[i.DbType] == nil {
			existingRDSParameters[i.DbType] = make(map[string]paramDetails)
		}
		existingRDSParameters[i.DbType][*param.ParameterName] = paramDetails{
			value:       *param.ParameterValue,
			applyMethod: string(param.ApplyMethod),
		}
	}
	return existingRDSParameters, nil
}

func (p *awsParameterGroupClient) getNewParameters(i *RDSInstance) (map[string]map[string]paramDetails, error) {
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

		// Configuration for the instnace to start generating logs if user enables these groups
		if slices.Contains(i.EnabledCloudwatchLogGroupExports, "general") ||
			slices.Contains(i.EnabledCloudwatchLogGroupExports, "slowquery") {
			customRDSParameters["mysql"]["log_output"] = paramDetails{
				value:       "FILE",
				applyMethod: "immediate",
			}
		}
		if slices.Contains(i.EnabledCloudwatchLogGroupExports, "general") {
			customRDSParameters["mysql"]["general_log"] = paramDetails{
				value:       "1",
				applyMethod: "immediate",
			}
		}
		if slices.Contains(i.EnabledCloudwatchLogGroupExports, "slowquery") {
			customRDSParameters["mysql"]["slow_query_log"] = paramDetails{
				value:       "1",
				applyMethod: "immediate",
			}
			if i.LongQueryTime != nil {
				customRDSParameters["mysql"]["long_query_time"] = paramDetails{
					value:       strconv.FormatFloat(*i.LongQueryTime, 'f', -1, 64),
					applyMethod: "immediate",
				}
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
		if q := i.PgQueryLogging; q != nil {
			if q.LogConnections != nil {
				customRDSParameters["postgres"]["log_connections"] = paramDetails{
					value:       logConnectionsToParamValue(*q.LogConnections),
					applyMethod: "immediate",
				}
			}
			if q.LogDisconnections != nil {
				customRDSParameters["postgres"]["log_disconnections"] = paramDetails{
					value:       boolToParamvalue(*q.LogDisconnections),
					applyMethod: "immediate",
				}
			}
			if q.LogCheckpoints != nil {
				customRDSParameters["postgres"]["log_checkpoints"] = paramDetails{
					value:       boolToParamvalue(*q.LogCheckpoints),
					applyMethod: "immediate",
				}
			}
			if q.LogLockWaits != nil {
				customRDSParameters["postgres"]["log_lock_waits"] = paramDetails{
					value:       boolToParamvalue(*q.LogLockWaits),
					applyMethod: "immediate",
				}
			}
			if q.LogMinDurationSample != nil {
				customRDSParameters["postgres"]["log_min_duration_sample"] = paramDetails{
					value:       strconv.FormatInt(*q.LogMinDurationSample, 10),
					applyMethod: "immediate",
				}
			}
			if q.LogMinDurationStatement != nil {
				customRDSParameters["postgres"]["log_min_duration_statement"] = paramDetails{
					value:       strconv.FormatInt(*q.LogMinDurationStatement, 10),
					applyMethod: "immediate",
				}
			}
			if q.LogStatement != nil {
				customRDSParameters["postgres"]["log_statement"] = paramDetails{
					value:       *q.LogStatement,
					applyMethod: "immediate",
				}
			}
			if q.LogStatementSampleRate != nil {
				customRDSParameters["postgres"]["log_statement_sample_rate"] = paramDetails{
					value:       strconv.FormatFloat(*q.LogStatementSampleRate, 'f', -1, 64),
					applyMethod: "immediate",
				}
			}
			if q.LogStatementStats != nil {
				customRDSParameters["postgres"]["log_statement_stats"] = paramDetails{
					value:       boolToParamvalue(*q.LogStatementStats),
					applyMethod: "immediate",
				}
			}

		}
	}

	return customRDSParameters, nil
}

func logConnectionsToParamValue(logConnections string) string {
	if logConnections == "true" {
		return "1"
	}
	if logConnections == "false" {
		return "0"
	}
	return logConnections
}

func boolToParamvalue(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// getParameterGroupName gets a parameter group name for the instance
func getParameterGroupName(i *RDSInstance, p *awsParameterGroupClient) string {
	// formatDBName() should always return the same value for the same database name,
	// so the parameter group name should remain consistent
	return p.parameterGroupPrefix + formatDBName(i.Database) + "-version-" + formatDBVersion(i.DbVersion)
}

func formatDBVersion(version string) string {
	return strings.ReplaceAll(version, ".", "-")
}

func (p *awsParameterGroupClient) DeleteParameterGroup(parameterGroupName string) error {
	if parameterGroupName == "" {
		p.logger.Info("could not delete parameter group: parameter group name was empty")
		return nil
	}

	exists, err := p.checkIfParameterGroupExists(parameterGroupName)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	attempts := 1
	maxRetries := int(p.settings.PollAwsMaxRetries)

	var parameterGroupIsDeleted bool

	for !parameterGroupIsDeleted && attempts <= maxRetries {
		_, err = p.rds.DeleteDBParameterGroup(p.ctx, &rds.DeleteDBParameterGroupInput{
			DBParameterGroupName: &parameterGroupName,
		})
		if err != nil {
			var invalidParameterGroupStateErr *rdsTypes.InvalidDBParameterGroupStateFault
			if errors.As(err, &invalidParameterGroupStateErr) {
				attempts += 1
				time.Sleep(p.settings.PollAwsMinDelay)
				continue
			}
			var notFoundErr *rdsTypes.DBParameterGroupNotFoundFault
			if errors.As(err, &notFoundErr) {
				parameterGroupIsDeleted = true
				return nil
			}
			return err
		}
	}

	if !parameterGroupIsDeleted {
		return errors.New("could not verify deletion of parameter group")
	}

	return err
}

// setParameterGroupName sets the parameter group name on the instance struct
func setParameterGroupName(i *RDSInstance, p *awsParameterGroupClient) {
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

func (p *awsParameterGroupClient) ReconcileRDSInstanceParameterGroup(dbInstanceState *rdsTypes.DBInstance, i RDSInstance) (*RDSInstance, error) {
	if len(dbInstanceState.DBParameterGroups) == 0 {
		return &i, nil
	}

	reconciledInstance := i
	parameterGroupName := *dbInstanceState.DBParameterGroups[0].DBParameterGroupName
	if !p.IsCustomParameterGroup(parameterGroupName) {
		return &reconciledInstance, nil
	}

	reconciledInstance.ParameterGroupName = parameterGroupName

	existingParameters, err := p.getExistingParameters(&reconciledInstance)
	if err != nil {
		return &reconciledInstance, nil
	}

	dbParameters, ok := existingParameters[reconciledInstance.DbType]
	if !ok {
		return &reconciledInstance, nil
	}

	addReconciledCloudwatchLogGroupExport := func(instance RDSInstance, enabledLogGroupName string) RDSInstance {
		if instance.EnabledCloudwatchLogGroupExports == nil {
			instance.EnabledCloudwatchLogGroupExports = make(pq.StringArray, 0)
		}

		idx, _ := slices.BinarySearch(instance.EnabledCloudwatchLogGroupExports, enabledLogGroupName)
		instance.EnabledCloudwatchLogGroupExports = slices.Insert(instance.EnabledCloudwatchLogGroupExports, idx, enabledLogGroupName)
		return instance
	}

	initPgQueryLogging := func(instance RDSInstance) RDSInstance {
		if instance.PgQueryLogging == nil {
			instance.PgQueryLogging = &PgQueryLoggingOptions{}
		}
		return instance
	}

	for key, paramDetails := range dbParameters {
		if key == "log_bin_trust_function_creators" {
			reconciledInstance.EnableFunctions = (paramDetails.value == "1")
		}
		if key == "binlog_format" {
			reconciledInstance.BinaryLogFormat = paramDetails.value
		}
		if key == "general_log" && paramDetails.value == "1" {
			reconciledInstance = addReconciledCloudwatchLogGroupExport(reconciledInstance, "general")
		}
		if key == "slow_query_log" && paramDetails.value == "1" {
			reconciledInstance = addReconciledCloudwatchLogGroupExport(reconciledInstance, "slowquery")
		}
		if key == "long_query_time" {
			longQueryTime, err := strconv.ParseFloat(paramDetails.value, 64)
			if err != nil {
				return &reconciledInstance, err
			}
			reconciledInstance.LongQueryTime = &longQueryTime
		}
		if key == sharedPreloadLibrariesParameterName {
			if strings.Contains(paramDetails.value, pgCronLibraryName) {
				reconciledInstance.EnablePgCron = aws.Bool(true)
			}
		}
		if key == "log_connections" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			reconciledInstance.PgQueryLogging.LogConnections = &paramDetails.value
		}
		if key == "log_disconnections" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			reconciledInstance.PgQueryLogging.LogDisconnections = aws.Bool(paramDetails.value == "1")
		}
		if key == "log_checkpoints" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			reconciledInstance.PgQueryLogging.LogCheckpoints = aws.Bool(paramDetails.value == "1")
		}
		if key == "log_lock_waits" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			reconciledInstance.PgQueryLogging.LogLockWaits = aws.Bool(paramDetails.value == "1")
		}
		if key == "log_min_duration_sample" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			logMinDurationSample, err := strconv.Atoi(paramDetails.value)
			if err != nil {
				return &reconciledInstance, err
			}
			reconciledInstance.PgQueryLogging.LogMinDurationSample = aws.Int64(int64(logMinDurationSample))
		}
		if key == "log_min_duration_statement" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			logMinDurationStatement, err := strconv.Atoi(paramDetails.value)
			if err != nil {
				return &reconciledInstance, err
			}
			reconciledInstance.PgQueryLogging.LogMinDurationStatement = aws.Int64(int64(logMinDurationStatement))
		}
		if key == "log_statement" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			reconciledInstance.PgQueryLogging.LogStatement = &paramDetails.value
		}
		if key == "log_statement_sample_rate" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			logStatementSampleRate, err := strconv.ParseFloat(paramDetails.value, 64)
			if err != nil {
				return &reconciledInstance, err
			}
			reconciledInstance.PgQueryLogging.LogStatementSampleRate = aws.Float64(logStatementSampleRate)
		}
		if key == "log_statement_stats" {
			reconciledInstance = initPgQueryLogging(reconciledInstance)
			reconciledInstance.PgQueryLogging.LogStatementStats = aws.Bool(paramDetails.value == "1")
		}
	}

	return &reconciledInstance, nil
}
