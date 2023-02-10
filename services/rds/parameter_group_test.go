package rds

import (
	"errors"
	"reflect"
	"testing"

	"github.com/18F/aws-broker/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
)

type mockRDSClient struct {
	rdsiface.RDSAPI

	dbEngineVersions                    []*rds.DBEngineVersion
	describeEngVersionsErr              error
	describeDbParamsErr                 error
	createDbParamGroupErr               error
	modifyDbParamGroupErr               error
	describeEngineDefaultParamsResults  []*rds.DescribeEngineDefaultParametersOutput
	describeEngineDefaultParamsErr      error
	describeEngineDefaultParamsNumPages int
	describeEngineDefaultParamsPageNum  int
	describeDbParamsResults             []*rds.DescribeDBParametersOutput
	describeDbParamsNumPages            int
	describeDbParamsPageNum             int
}

func (m mockRDSClient) DescribeDBParameters(*rds.DescribeDBParametersInput) (*rds.DescribeDBParametersOutput, error) {
	if m.describeDbParamsErr != nil {
		return nil, m.describeDbParamsErr
	}
	return nil, nil
}

func (m mockRDSClient) DescribeDBEngineVersions(*rds.DescribeDBEngineVersionsInput) (*rds.DescribeDBEngineVersionsOutput, error) {
	if m.dbEngineVersions != nil {
		return &rds.DescribeDBEngineVersionsOutput{
			DBEngineVersions: m.dbEngineVersions,
		}, nil
	}
	if m.describeEngVersionsErr != nil {
		return nil, m.describeEngVersionsErr
	}
	return nil, nil
}

func (m mockRDSClient) CreateDBParameterGroup(*rds.CreateDBParameterGroupInput) (*rds.CreateDBParameterGroupOutput, error) {
	if m.createDbParamGroupErr != nil {
		return nil, m.createDbParamGroupErr
	}
	return nil, nil
}

func (m mockRDSClient) ModifyDBParameterGroup(*rds.ModifyDBParameterGroupInput) (*rds.DBParameterGroupNameMessage, error) {
	if m.modifyDbParamGroupErr != nil {
		return nil, m.modifyDbParamGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) DescribeEngineDefaultParametersPages(input *rds.DescribeEngineDefaultParametersInput, fn func(*rds.DescribeEngineDefaultParametersOutput, bool) bool) error {
	if m.describeEngineDefaultParamsErr != nil {
		return m.describeEngineDefaultParamsErr
	}
	shouldContinue := true
	for shouldContinue {
		output := m.describeEngineDefaultParamsResults[m.describeEngineDefaultParamsPageNum]
		m.describeEngineDefaultParamsPageNum++
		lastPage := m.describeEngineDefaultParamsPageNum == m.describeEngineDefaultParamsNumPages
		shouldContinue = fn(output, lastPage)
	}
	return nil
}

func (m *mockRDSClient) DescribeDBParametersPages(input *rds.DescribeDBParametersInput, fn func(*rds.DescribeDBParametersOutput, bool) bool) error {
	if m.describeDbParamsErr != nil {
		return m.describeDbParamsErr
	}
	shouldContinue := true
	for shouldContinue {
		output := m.describeDbParamsResults[m.describeDbParamsPageNum]
		m.describeDbParamsPageNum++
		lastPage := m.describeDbParamsPageNum == m.describeDbParamsNumPages
		shouldContinue = fn(output, lastPage)
	}
	return nil
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
	i := &RDSInstance{
		Database: "db1234",
	}
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
			},
			expectedParameterGroupName: "prefix-db1234",
		},
		"has existing value": {
			parameterGroupAdapter: &awsParameterGroupClient{},
			dbInstance: &RDSInstance{
				ParameterGroupName: "param-group-1234",
			},
			expectedParameterGroupName: "param-group-1234",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			setParameterGroupName(test.dbInstance, test.parameterGroupAdapter)
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
			dbInstance: &RDSInstance{},
			expectedOk: false,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
		"valid binary log format": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
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
				EnablePgCron: true,
				DbType:       "postgres",
			},
			expectedOk: true,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
		"disable PG cron": {
			dbInstance: &RDSInstance{
				DisablePgCron: true,
				DbType:        "postgres",
			},
			expectedOk: true,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			if test.parameterGroupAdapter.needCustomParameters(test.dbInstance) != test.expectedOk {
				t.Fatalf("should be %v", test.expectedOk)
			}
		})
	}
}

func TestGetDefaultEngineParameterValue(t *testing.T) {
	describeEngineDefaultParamsErr := errors.New("describe db engine default params err")
	describeEngVersionsErr := errors.New("describe eng versions err")
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
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedParamValue: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{},
							},
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
			},
			expectedGetDefaultEngineParamsCalls: 1,
		},
		"default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedParamValue: "random-library",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("random-library"),
									},
								},
							},
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
			},
			expectedGetDefaultEngineParamsCalls: 1,
		},
		"default param value, with paging": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName: "shared_preload_libraries",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("random-param"),
										ParameterValue: aws.String("random-value"),
									},
								},
								Marker: aws.String("marker"),
							},
						},
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("a-library,b-library"),
									},
								},
							},
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
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName:          "shared_preload_libraries",
			expectedErr:        describeEngineDefaultParamsErr,
			expectedParamValue: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineDefaultParamsErr,
				},
			},
		},
		"describe db engine versions error": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
			},
			paramName:          "shared_preload_libraries",
			expectedErr:        describeEngVersionsErr,
			expectedParamValue: "",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngVersionsErr: describeEngVersionsErr,
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{},
							},
						},
					},
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			parameterValue, err := test.parameterGroupAdapter.getDefaultEngineParameterValue(test.dbInstance, test.paramName)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
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
		parameters             []*rds.Parameter
		parameterName          string
		expectedParameterValue string
	}{
		"finds value": {
			parameters: []*rds.Parameter{
				{
					ParameterName:  aws.String("foo"),
					ParameterValue: aws.String("bar"),
				},
			},
			parameterName:          "foo",
			expectedParameterValue: "bar",
		},
		"does not find value": {
			parameters: []*rds.Parameter{
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
							Parameters: []*rds.Parameter{},
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
							Parameters: []*rds.Parameter{
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
							Parameters: []*rds.Parameter{
								{
									ParameterName:  aws.String("moo"),
									ParameterValue: aws.String("cow"),
								},
							},
						},
						{
							Parameters: []*rds.Parameter{
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
			parameterValue, err := test.parameterGroupAdapter.getCustomParameterValue(test.dbInstance, test.parameterName)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
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
		dbInstance            *RDSInstance
		customLibrary         string
		expectedParam         string
		currentParameterValue string
		expectedErr           error
	}{
		"no default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			currentParameterValue: "",
			customLibrary:         "library1",
			expectedParam:         "library1",
		},
		"has default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			currentParameterValue: "library1",
			customLibrary:         "library2",
			expectedParam:         "library2,library1",
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
		dbInstance             *RDSInstance
		customLibrary          string
		currentParameterValue  string
		expectedParameterValue string
	}{
		"returns empty default": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			currentParameterValue:  "",
			customLibrary:          "pg_cron",
			expectedParameterValue: "",
		},
		"removes value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
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
				EnablePgCron: true,
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
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{},
							},
						},
					},
					describeEngineDefaultParamsNumPages: 1,
				},
			},
		},
		"enable PG cron, existing parameter group": {
			dbInstance: &RDSInstance{
				EnablePgCron:       true,
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
							Parameters: []*rds.Parameter{},
						},
					},
					describeDbParamsNumPages: 1,
				},
			},
		},
		"disable PG cron, no existing parameter group": {
			dbInstance: &RDSInstance{
				DisablePgCron: true,
				DbType:        "postgres",
				DbVersion:     "12",
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
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("test"),
										ParameterValue: aws.String("moo"),
									},
								},
							},
						},
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("foo,bar"),
									},
								},
							},
						},
					},
					describeEngineDefaultParamsNumPages: 2,
				},
			},
		},
		"disable PG cron, existing parameter group": {
			dbInstance: &RDSInstance{
				DisablePgCron:      true,
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
							Parameters: []*rds.Parameter{
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
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: nil,
			expectedErr:    describeEngineParamsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineParamsErr,
				},
			},
		},
		"enable PG cron, describe db params error": {
			dbInstance: &RDSInstance{
				EnablePgCron:       true,
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
				DisablePgCron: true,
				DbType:        "postgres",
				DbVersion:     "12",
			},
			expectedParams: nil,
			expectedErr:    describeEngineParamsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				settings: config.Settings{},
				rds: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineParamsErr,
				},
			},
		},
		"disable PG cron, describe db params error": {
			dbInstance: &RDSInstance{
				DisablePgCron:      true,
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
			params, err := test.parameterGroupAdapter.getCustomParameters(test.dbInstance)
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

func TestGetParameterGroupFamily(t *testing.T) {
	serviceErr := errors.New("fail")
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedErr           error
		expectedPGroupFamily  string
		parameterGroupAdapter *awsParameterGroupClient
	}{
		"no db version": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedPGroupFamily: "postgres12",
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					dbEngineVersions: []*rds.DBEngineVersion{
						{
							DBParameterGroupFamily: aws.String("postgres12"),
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
				rds: &mockRDSClient{},
			},
		},
		"RDS service returns error": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedErr: serviceErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngVersionsErr: serviceErr,
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
			err := test.parameterGroupAdapter.getParameterGroupFamily(test.dbInstance)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
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
	}{
		"error, return false": {
			dbInstance: &RDSInstance{
				ParameterGroupName: "group1",
			},
			expectedExists: false,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr: dbParamsErr,
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
			exists := test.parameterGroupAdapter.checkIfParameterGroupExists(test.dbInstance.ParameterGroupName)
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
			},
			expectedErr: describeEngVersionsErr,
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeDbParamsErr:    errors.New("describe DB params err"),
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
					describeDbParamsErr:   errors.New("describe DB params err"),
					createDbParamGroupErr: createDbParamGroupErr,
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
				rds: &mockRDSClient{},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.parameterGroupAdapter.createOrModifyCustomParameterGroup(test.dbInstance, nil)
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
				EnablePgCron: true,
				Database:     "database2",
			},
			parameterGroupAdapter: &awsParameterGroupClient{
				rds: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("random-param"),
										ParameterValue: aws.String("random-value"),
									},
								},
							},
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
							Parameters: []*rds.Parameter{
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
				test.dbInstance,
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
