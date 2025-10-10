package rds

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/config"
)

func createTestRdsInstance(i *RDSInstance) *RDSInstance {
	i.dbUtils = &RDSDatabaseUtils{}
	return i
}

func TestNewParameterGroupAdapter(t *testing.T) {
	parameterGroupAdapter := NewAwsParameterGroupClient(
		&mockRDSClient{},
		config.Settings{},
	)
	if parameterGroupAdapter.parameterGroupPrefix != "cg-aws-broker-" {
		t.Errorf("actual prefix: %s", parameterGroupAdapter.parameterGroupPrefix)
	}
}

func TestGetParameterGroupName(t *testing.T) {
	p := &awsParameterGroupClient{
		parameterGroupPrefix: "prefix-",
	}
	i := createTestRdsInstance(&RDSInstance{
		Database: "db1234",
	})
	parameterGroupName := getParameterGroupName(i, p)
	expectedParameterGroupName := "prefix-db1234"
	if parameterGroupName != expectedParameterGroupName {
		t.Errorf("got parameter group name: %s, expected %s", parameterGroupName, expectedParameterGroupName)
	}
}

func TestSetParameterGroupName(t *testing.T) {
	testCases := map[string]struct {
		dbInstance                 *RDSInstance
		expectedParameterGroupName string
		parameterGroupAdapter      *awsParameterGroupClient
	}{
		"no existing value": {
			parameterGroupAdapter: &awsParameterGroupClient{
				parameterGroupPrefix: "prefix-",
			},
			dbInstance: &RDSInstance{
				Database: "db1234",
				dbUtils:  &RDSDatabaseUtils{},
			},
			expectedParameterGroupName: "prefix-db1234",
		},
		"has existing value": {
			parameterGroupAdapter: &awsParameterGroupClient{},
			dbInstance: &RDSInstance{
				ParameterGroupName: "param-group-1234",
				dbUtils:            &RDSDatabaseUtils{},
			},
			expectedParameterGroupName: "param-group-1234",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			setParameterGroupName(createTestRdsInstance(test.dbInstance), test.parameterGroupAdapter)
			if test.dbInstance.ParameterGroupName != test.expectedParameterGroupName {
				t.Errorf("got parameter group name: %s, expected %s", test.dbInstance.ParameterGroupName, test.expectedParameterGroupName)
			}
		})
	}
}

func TestNeedCustomParameters(t *testing.T) {
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedOk            bool
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"default": {
			dbInstance: &RDSInstance{
				dbUtils: &RDSDatabaseUtils{},
			},
			expectedOk: false,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
		"valid binary log format": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
			expectedOk: true,
		},
		"valid binary log format, wrong database type": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "psql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
			expectedOk: false,
		},
		"instance functions enabled, settings disabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{
					EnableFunctionsFeature: false,
				},
			},
			expectedOk: false,
		},
		"instance functions disabled, settings enabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: false,
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{
					EnableFunctionsFeature: true,
				},
			},
			expectedOk: false,
		},
		"instance functions enabled, settings enabled, wrong db type": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "psql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{
					EnableFunctionsFeature: true,
				},
			},
			expectedOk: false,
		},
		"instance functions enabled, settings enabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{
					EnableFunctionsFeature: true,
				},
			},
			expectedOk: true,
		},
		"enable PG cron": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				dbUtils:      &RDSDatabaseUtils{},
			},
			expectedOk: true,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
		"disable PG cron": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(false),
				DbType:       "postgres",
				dbUtils:      &RDSDatabaseUtils{},
			},
			expectedOk: true,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
		"enable PG cron not specified": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedOk: false,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			if test.parameterGroupAdapter.needCustomParameters(createTestRdsInstance(test.dbInstance)) != test.expectedOk {
				t.Fatalf("should be %v", test.expectedOk)
			}
		})
	}
}

func TestGetDefaultEngineParameterValue(t *testing.T) {
	describeEngineDefaultParamsErr := &rdsTypes.DBParameterGroupNotFoundFault{}
	describeEngVersionsErr := &rdsTypes.DBParameterGroupNotFoundFault{}

	testCases := map[string]struct {
		dbInstance                          *RDSInstance
		paramName                           string
		expectedParamValue                  string
		expectedErr                         error
		parameterGroupAdapter               *awsParameterGroupClient
		expectedGetDefaultEngineParamsCalls int
	}{
		"no default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedParamValue: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
			},
			expectedGetDefaultEngineParamsCalls: 1,
		},
		"default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedParamValue: "random-library",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("random-library"),
									},
								},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
			},
			expectedGetDefaultEngineParamsCalls: 1,
		},
		"default param value, with paging": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName: "shared_preload_libraries",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{
									{
										ParameterName:  aws.String("random-param"),
										ParameterValue: aws.String("random-value"),
									},
								},
								Marker: aws.String("marker"),
							},
						},
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("a-library,b-library"),
									},
								},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
					describeEngineDefaultParamsNumPages: 2,
				},
			},
			expectedParamValue:                  "a-library,b-library",
			expectedGetDefaultEngineParamsCalls: 2,
		},
		"describe db engine params error": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedErr:        describeEngineDefaultParamsErr,
			expectedParamValue: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineDefaultParamsErr,
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
				},
			},
		},
		"describe db engine versions error": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedParamValue: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngVersionsErr: describeEngVersionsErr,
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{},
							},
						},
					},
				},
			},
			expectedErr: describeEngVersionsErr,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			parameterValue, err := test.parameterGroupAdapter.getDefaultEngineParameterValue(
				createTestRdsInstance(test.dbInstance),
				test.paramName,
			)
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error type: %s, got: %s", test.expectedErr, err)
			}
			if parameterValue != test.expectedParamValue {
				t.Errorf("expected: %s, got: %s", test.expectedParamValue, parameterValue)
			}
			mockClient, ok := test.parameterGroupAdapter.rds.(*mockRDSClient)
			if ok {
				if mockClient.describeEngineDefaultParamsPageNum != test.expectedGetDefaultEngineParamsCalls {
					t.Fatalf("expected %v, got %v", test.expectedGetDefaultEngineParamsCalls, mockClient.describeEngineDefaultParamsPageNum)
				}
			}
		})
	}
}

func TestFindParameterValueInResults(t *testing.T) {
	testCases := map[string]struct {
		parameters             []rdsTypes.Parameter
		parameterName          string
		expectedParameterValue string
	}{
		"finds value": {
			parameters: []rdsTypes.Parameter{
				{
					ParameterName:  aws.String("foo"),
					ParameterValue: aws.String("bar"),
				},
			},
			parameterName:          "foo",
			expectedParameterValue: "bar",
		},
		"does not find value": {
			parameters: []rdsTypes.Parameter{
				{
					ParameterName:  aws.String("moo"),
					ParameterValue: aws.String("cow"),
				},
			},
			parameterName:          "foo",
			expectedParameterValue: "",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			parameterValue := findParameterValueInResults(test.parameters, test.parameterName)
			if parameterValue != test.expectedParameterValue {
				t.Errorf("expected: %s, got: %s", test.expectedParameterValue, parameterValue)
			}
		})
	}
}

func TestGetCustomParameterValue(t *testing.T) {
	describeDbParamsError := errors.New("describe db params error")
	testCases := map[string]struct {
		dbInstance             *RDSInstance
		parameterGroupAdapter  *awsParameterGroupClient
		parameterName          string
		expectedParameterValue string
		expectedErr            error
		expectedNumPages       int
	}{
		"no value": {
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsResults: []*rds.DescribeDBParametersOutput{
						{
							Parameters: []rdsTypes.Parameter{},
						},
					},
					describeDbParamsNumPages: 1,
				},
			},
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "12",
			},
			parameterName:          "foo",
			expectedParameterValue: "",
			expectedNumPages:       1,
		},
		"gets value": {
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsResults: []*rds.DescribeDBParametersOutput{
						{
							Parameters: []rdsTypes.Parameter{
								{
									ParameterName:  aws.String("foo"),
									ParameterValue: aws.String("bar"),
								},
							},
						},
					},
					describeDbParamsNumPages: 1,
				},
			},
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "12",
			},
			parameterName:          "foo",
			expectedParameterValue: "bar",
			expectedNumPages:       1,
		},
		"gets value, with paging": {
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsResults: []*rds.DescribeDBParametersOutput{
						{
							Parameters: []rdsTypes.Parameter{
								{
									ParameterName:  aws.String("moo"),
									ParameterValue: aws.String("cow"),
								},
							},
							Marker: aws.String("another page"),
						},
						{
							Parameters: []rdsTypes.Parameter{
								{
									ParameterName:  aws.String("foo"),
									ParameterValue: aws.String("bar"),
								},
							},
						},
					},
					describeDbParamsNumPages: 2,
				},
			},
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "12",
			},
			parameterName:          "foo",
			expectedParameterValue: "bar",
			expectedNumPages:       2,
		},
		"error getting DB params": {
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr: describeDbParamsError,
				},
			},
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "12",
			},
			parameterName: "foo",
			expectedErr:   describeDbParamsError,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			parameterValue, err := test.parameterGroupAdapter.getCustomParameterValue(
				createTestRdsInstance(test.dbInstance),
				test.parameterName,
			)
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if parameterValue != test.expectedParameterValue {
				t.Errorf("expected: %s, got: %s", test.expectedParameterValue, parameterValue)
			}
			mockClient, ok := test.parameterGroupAdapter.rds.(*mockRDSClient)
			if ok {
				if mockClient.describeDbParamsPageNum != test.expectedNumPages {
					t.Fatalf("expected %v, got %v", test.expectedNumPages, mockClient.describeDbParamsPageNum)
				}
			}
		})
	}
}

func TestAddLibraryToSharedPreloadLibraries(t *testing.T) {
	testCases := map[string]struct {
		customLibrary         string
		expectedParam         string
		currentParameterValue string
		expectedErr           error
	}{
		"no default param value": {
			currentParameterValue: "",
			customLibrary:         "library1",
			expectedParam:         "library1",
		},
		"has default param value": {
			currentParameterValue: "library1",
			customLibrary:         "library2",
			expectedParam:         "library1,library2",
		},
		"param value already exists": {
			currentParameterValue: "library1,library2",
			customLibrary:         "library2",
			expectedParam:         "library1,library2",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			param := addLibraryToSharedPreloadLibraries(test.currentParameterValue, test.customLibrary)
			if param != test.expectedParam {
				t.Fatalf("expected %s, got: %s", test.expectedParam, param)
			}
		})
	}
}

func TestRemoveLibraryFromSharedPreloadLibraries(t *testing.T) {
	testCases := map[string]struct {
		customLibrary          string
		currentParameterValue  string
		expectedParameterValue string
	}{
		"returns empty default": {
			currentParameterValue:  "",
			customLibrary:          "pg_cron",
			expectedParameterValue: "",
		},
		"removes value": {
			currentParameterValue:  "a,b,pg_cron",
			customLibrary:          "pg_cron",
			expectedParameterValue: "a,b",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			parameterValue := removeLibraryFromSharedPreloadLibraries(test.currentParameterValue, test.customLibrary)
			if parameterValue != test.expectedParameterValue {
				t.Fatalf("expected %s, got: %s", test.expectedParameterValue, parameterValue)
			}
		})
	}
}

func TestGetCustomParameters(t *testing.T) {
	describeEngineParamsErr := errors.New("describe db engine params error")
	describeDbParamsErr := errors.New("describe db params error")
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedParams        map[string]map[string]paramDetails
		expectedErr           error
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"enabled functions": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			expectedParams: map[string]map[string]paramDetails{
				"mysql": {
					"log_bin_trust_function_creators": paramDetails{
						value:       "1",
						applyMethod: "immediate",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
				settings: config.Settings{
					EnableFunctionsFeature: true,
				},
			},
		},
		"instance functions disabled, settings enabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: false,
				DbType:          "mysql",
			},
			expectedParams: map[string]map[string]paramDetails{
				"mysql": {
					"log_bin_trust_function_creators": paramDetails{
						value:       "0",
						applyMethod: "immediate",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
				settings: config.Settings{
					EnableFunctionsFeature: true,
				},
			},
		},
		"instance functions enabled, settings disabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			expectedParams: map[string]map[string]paramDetails{
				"mysql": {
					"log_bin_trust_function_creators": paramDetails{
						value:       "0",
						applyMethod: "immediate",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
				settings: config.Settings{
					EnableFunctionsFeature: false,
				},
			},
		},
		"binary log format": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
			},
			expectedParams: map[string]map[string]paramDetails{
				"mysql": {
					"log_bin_trust_function_creators": paramDetails{
						value:       "0",
						applyMethod: "immediate",
					},
					"binlog_format": paramDetails{
						value:       "ROW",
						applyMethod: "immediate",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds:      &mockRDSClient{},
				settings: config.Settings{},
			},
		},
		"enable PG cron, no existing parameter group": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: map[string]map[string]paramDetails{
				"postgres": {
					"shared_preload_libraries": paramDetails{
						value:       "pg_cron",
						applyMethod: "pending-reboot",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
			},
		},
		"enable PG cron, existing parameter group": {
			dbInstance: &RDSInstance{
				EnablePgCron:       aws.Bool(true),
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "group1",
			},
			expectedParams: map[string]map[string]paramDetails{
				"postgres": {
					"shared_preload_libraries": paramDetails{
						value:       "pg_cron",
						applyMethod: "pending-reboot",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeDbParamsResults: []*rds.DescribeDBParametersOutput{
						{
							Parameters: []rdsTypes.Parameter{},
						},
					},
					describeDbParamsNumPages: 1,
				},
			},
		},
		"disable PG cron, no existing parameter group": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(false),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: map[string]map[string]paramDetails{
				"postgres": {
					"shared_preload_libraries": paramDetails{
						value:       "foo,bar",
						applyMethod: "pending-reboot",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{
									{
										ParameterName:  aws.String("test"),
										ParameterValue: aws.String("moo"),
									},
								},
								Marker: aws.String("next page"),
							},
						},
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("foo,bar"),
									},
								},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
					describeEngineDefaultParamsNumPages: 2,
				},
			},
		},
		"disable PG cron, existing parameter group": {
			dbInstance: &RDSInstance{
				EnablePgCron:       aws.Bool(false),
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "group1",
			},
			expectedParams: map[string]map[string]paramDetails{
				"postgres": {
					"shared_preload_libraries": paramDetails{
						value:       "foo,bar",
						applyMethod: "pending-reboot",
					},
				},
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeDbParamsResults: []*rds.DescribeDBParametersOutput{
						{
							Parameters: []rdsTypes.Parameter{
								{
									ParameterName:  aws.String("shared_preload_libraries"),
									ParameterValue: aws.String("pg_cron,foo,bar"),
								},
							},
						},
					},
					describeDbParamsNumPages: 1,
				},
			},
		},
		"enable PG cron, describe db default params error": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: nil,
			expectedErr:    describeEngineParamsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineParamsErr,
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
				},
			},
		},
		"enable PG cron, describe db params error": {
			dbInstance: &RDSInstance{
				EnablePgCron:       aws.Bool(true),
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "group1",
			},
			expectedParams: nil,
			expectedErr:    describeDbParamsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeDbParamsErr: describeDbParamsErr,
				},
			},
		},
		"disable PG cron, describe db default params error": {
			dbInstance: &RDSInstance{
				EnablePgCron: aws.Bool(false),
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: nil,
			expectedErr:    describeEngineParamsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineParamsErr,
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
				},
			},
		},
		"disable PG cron, describe db params error": {
			dbInstance: &RDSInstance{
				EnablePgCron:       aws.Bool(false),
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "group1",
			},
			expectedParams: nil,
			expectedErr:    describeDbParamsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeDbParamsErr: describeDbParamsErr,
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := test.parameterGroupAdapter.getCustomParameters(createTestRdsInstance(test.dbInstance))
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if !reflect.DeepEqual(params, test.expectedParams) {
				t.Fatalf("expected %s, got: %s", test.expectedParams, params)
			}
		})
	}
}

func TestGetDatabaseEngineVersion(t *testing.T) {
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedErr           string
		expectedDbVersion     string
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"no db version, fetches version from database name": {
			dbInstance: &RDSInstance{
				DbType:   "postgres",
				Database: "database1",
			},
			expectedDbVersion: "version1",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									EngineVersion: aws.String("version1"),
								},
							},
						},
					},
				},
			},
		},
		"no db version, no database name": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedErr: "database name is required to get database engine version",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
			},
		},
		"no db version, error getting database instance info": {
			dbInstance: &RDSInstance{
				DbType:   "postgres",
				Database: "database1",
			},
			expectedErr: "describe db instances error",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("describe db instances error")},
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dbVersion, err := test.parameterGroupAdapter.getDatabaseEngineVersion(createTestRdsInstance(test.dbInstance))
			if test.expectedErr == "" && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if err != nil && test.expectedErr != "" && test.expectedErr != err.Error() {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if dbVersion != test.expectedDbVersion {
				t.Fatalf("expected parameter group family: %s, got: %s", test.expectedDbVersion, dbVersion)
			}
		})
	}
}

func TestGetParameterGroupFamily(t *testing.T) {
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedErr           string
		expectedPGroupFamily  string
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"no db version": {
			dbInstance: &RDSInstance{
				DbType:   "postgres",
				Database: "database1",
			},
			expectedPGroupFamily: "postgres1",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									EngineVersion: aws.String("version1"),
								},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres1"),
						},
					},
				},
			},
		},
		"has db version": {
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "13",
			},
			expectedPGroupFamily: "postgres13",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres13"),
						},
					},
				},
			},
		},
		"RDS service returns error": {
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "12",
			},
			expectedErr: "fail",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngVersionsErr: errors.New("fail"),
				},
			},
		},
		"instance has parameter group family": {
			dbInstance: &RDSInstance{
				ParameterGroupFamily: "random-family",
			},
			expectedPGroupFamily: "random-family",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.parameterGroupAdapter.getParameterGroupFamily(createTestRdsInstance(test.dbInstance))
			if test.expectedErr == "" && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if err != nil && test.expectedErr != "" && test.expectedErr != err.Error() {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if test.dbInstance.ParameterGroupFamily != test.expectedPGroupFamily {
				t.Fatalf("expected parameter group family: %s, got: %s", test.expectedPGroupFamily, test.dbInstance.ParameterGroupFamily)
			}
		})
	}
}

func TestCheckIfParameterGroupExists(t *testing.T) {
	dbParamsErr := errors.New("fail")

	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedExists        bool
		parameterGroupAdapter *awsParameterGroupClient
		expectedErr           error
	}{
		"unexpected error, return false": {
			dbInstance: &RDSInstance{
				ParameterGroupName: "group1",
			},
			expectedExists: false,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr: dbParamsErr,
				},
			},
			expectedErr: dbParamsErr,
		},
		"not found error, return false": {
			dbInstance: &RDSInstance{
				ParameterGroupName: "group1",
			},
			expectedExists: false,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr: &rdsTypes.DBParameterGroupNotFoundFault{},
				},
			},
		},
		"no error, return true": {
			dbInstance: &RDSInstance{
				ParameterGroupName: "group2",
			},
			expectedExists: true,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			exists, err := test.parameterGroupAdapter.checkIfParameterGroupExists(test.dbInstance.ParameterGroupName)
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if exists != test.expectedExists {
				t.Fatalf("expected: %t, got: %t", test.expectedExists, exists)
			}
		})
	}
}

func TestCreateOrModifyCustomParameterGroup(t *testing.T) {
	createDbParamGroupErr := errors.New("create DB params err")
	describeEngVersionsErr := errors.New("describe DB engine versions err")
	modifyDbParamGroupErr := errors.New("modify DB params err")

	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedErr           error
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"error getting parameter group family": {
			dbInstance: &RDSInstance{
				Database:           "foobar",
				DbType:             "postgres",
				ParameterGroupName: "foobar",
				DbVersion:          "12",
			},
			expectedErr: describeEngVersionsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr:    &rdsTypes.DBParameterGroupNotFoundFault{},
					describeEngVersionsErr: describeEngVersionsErr,
				},
			},
		},
		"error creating database parameter group": {
			dbInstance: &RDSInstance{
				Database:           "foobar",
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "foobar",
			},
			expectedErr: createDbParamGroupErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr:   &rdsTypes.DBParameterGroupNotFoundFault{},
					createDbParamGroupErr: createDbParamGroupErr,
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
				},
			},
		},
		"error modifying database parameter group": {
			dbInstance: &RDSInstance{
				Database:           "foobar",
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "foobar",
			},
			expectedErr: modifyDbParamGroupErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					modifyDbParamGroupErr: modifyDbParamGroupErr,
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
				},
			},
		},
		"success": {
			dbInstance: &RDSInstance{
				Database:           "foobar",
				DbType:             "postgres",
				DbVersion:          "12",
				ParameterGroupName: "foobar",
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.parameterGroupAdapter.createOrModifyCustomParameterGroup(createTestRdsInstance(test.dbInstance), nil, nil)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
		})
	}
}

func TestProvisionCustomParameterGroupIfNecessary(t *testing.T) {
	modifyDbParamGroupErr := errors.New("create DB param group error")
	testCases := map[string]struct {
		customParams          map[string]map[string]string
		dbInstance            *RDSInstance
		expectedPGroupName    string
		expectedErr           error
		dedicatedDBAdapter    *dedicatedDBAdapter
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"does not need custom params": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedPGroupName: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{},
			},
		},
		"enable binary log format, success": {
			dbInstance: &RDSInstance{
				DbType:          "mysql",
				BinaryLogFormat: "ROW",
				Database:        "database1",
			},
			expectedPGroupName: "prefix-database1",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds:                  &mockRDSClient{},
				parameterGroupPrefix: "prefix-",
			},
		},
		"enable PG cron, success": {
			dbInstance: &RDSInstance{
				DbType:       "postgres",
				DbVersion:    "12",
				EnablePgCron: aws.Bool(true),
				Database:     "database2",
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rdsTypes.EngineDefaults{
								Parameters: []rdsTypes.Parameter{
									{
										ParameterName:  aws.String("random-param"),
										ParameterValue: aws.String("random-value"),
									},
								},
							},
						},
					},
					dbEngineVersions: []rdsTypes.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
				parameterGroupPrefix: "prefix-",
			},
			expectedPGroupName: "prefix-database2",
		},
		"needs custom params, error": {
			dbInstance: &RDSInstance{
				DbType:             "mysql",
				BinaryLogFormat:    "ROW",
				Database:           "database1",
				ParameterGroupName: "group1",
			},
			expectedErr: modifyDbParamGroupErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					modifyDbParamGroupErr: modifyDbParamGroupErr,
					describeDbParamsResults: []*rds.DescribeDBParametersOutput{
						{
							Parameters: []rdsTypes.Parameter{
								{
									ParameterName:  aws.String("random-param"),
									ParameterValue: aws.String("random-value"),
								},
							},
						},
					},
					describeDbParamsNumPages: 1,
				},
			},
			expectedPGroupName: "group1",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.parameterGroupAdapter.ProvisionCustomParameterGroupIfNecessary(
				createTestRdsInstance(test.dbInstance),
				nil,
			)

			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if test.dbInstance.ParameterGroupName != test.expectedPGroupName {
				t.Fatalf("unexpected group name: %s, expected: %s", test.dbInstance.ParameterGroupName, test.expectedPGroupName)
			}
		})
	}
}
