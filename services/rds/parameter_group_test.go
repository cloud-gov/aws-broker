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

	dbEngineVersions                   []*rds.DBEngineVersion
	describeEngVersionsErr             error
	describeDbParamsErr                error
	createDbParamGroupErr              error
	modifyDbParamGroupErr              error
	describeEngineDefaultParamsResults []*rds.DescribeEngineDefaultParametersOutput
	describeEngineDefaultParamsErr     error
	describeEngineDefaultParamsCallNum int
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

func (m *mockRDSClient) DescribeEngineDefaultParameters(*rds.DescribeEngineDefaultParametersInput) (*rds.DescribeEngineDefaultParametersOutput, error) {
	if m.describeEngineDefaultParamsErr != nil {
		return nil, m.describeEngineDefaultParamsErr
	}
	if m.describeEngineDefaultParamsResults != nil {
		res := m.describeEngineDefaultParamsResults[m.describeEngineDefaultParamsCallNum]
		m.describeEngineDefaultParamsCallNum++
		return res, nil
	}
	return nil, nil
}

func TestNeedCustomParameters(t *testing.T) {
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedOk            bool
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"default": {
			dbInstance: &RDSInstance{},
			expectedOk: false,
			parameterGroupAdapter: &parameterGroupAdapter{
				settings: config.Settings{},
			},
		},
		"valid binary log format": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				settings: config.Settings{},
			},
			expectedOk: true,
		},
		"valid binary log format, wrong database type": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "psql",
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				settings: config.Settings{},
			},
			expectedOk: false,
		},
		"instance functions enabled, settings disabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			parameterGroupAdapter: &parameterGroupAdapter{
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
			parameterGroupAdapter: &parameterGroupAdapter{
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
			parameterGroupAdapter: &parameterGroupAdapter{
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
			parameterGroupAdapter: &parameterGroupAdapter{
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
			parameterGroupAdapter: &parameterGroupAdapter{
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

func TestGetDefaultEngineParameter(t *testing.T) {
	describeEngineDefaultParamsErr := errors.New("describe db engine default params err")
	describeEngVersionsErr := errors.New("describe eng versions err")
	testCases := map[string]struct {
		dbInstance                          *RDSInstance
		expectedParams                      map[string]map[string]string
		paramName                           string
		expectedParamValue                  string
		expectedErr                         error
		parameterGroupAdapter               *parameterGroupAdapter
		expectedGetDefaultEngineParamsCalls int
	}{
		"no default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			paramName: "shared_preload_libraries",
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libaries": "pg-cron",
				},
			},
			expectedParamValue: "",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{},
							},
						},
					},
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
			paramName: "shared_preload_libraries",
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libaries": "pg-cron",
				},
			},
			expectedParamValue: "random-library",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
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
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libaries": "pg-cron",
				},
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
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
			paramName: "shared_preload_libraries",
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libaries": "pg-cron",
				},
			},
			expectedErr:        describeEngineDefaultParamsErr,
			expectedParamValue: "",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineDefaultParamsErr,
				},
			},
		},
		"describe db engine versions error": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
			},
			paramName: "shared_preload_libraries",
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libaries": "pg-cron",
				},
			},
			expectedErr:        describeEngVersionsErr,
			expectedParamValue: "",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
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
			paramValue, err := test.parameterGroupAdapter.getDefaultEngineParameter(test.paramName, test.dbInstance)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if paramValue != test.expectedParamValue {
				t.Errorf("expected: %s, got: %s", test.expectedParamValue, paramValue)
			}
			mockClient, ok := test.parameterGroupAdapter.svc.(*mockRDSClient)
			if ok {
				if mockClient.describeEngineDefaultParamsCallNum != test.expectedGetDefaultEngineParamsCalls {
					t.Fatalf("expected %v, got %v", test.expectedGetDefaultEngineParamsCalls, mockClient.describeEngineDefaultParamsCallNum)
				}
			}
		})
	}
}

func TestBuildCustomSharePreloadLibrariesParam(t *testing.T) {
	describeEngineParamsErr := errors.New("describe engine params error")
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		customLibrary         string
		expectedParam         string
		expectedErr           error
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"no default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{},
							},
						},
					},
				},
			},
			customLibrary: "library1",
			expectedParam: "library1",
		},
		"has default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
						{
							EngineDefaults: &rds.EngineDefaults{
								Parameters: []*rds.Parameter{
									{
										ParameterName:  aws.String("shared_preload_libraries"),
										ParameterValue: aws.String("library1"),
									},
								},
							},
						},
					},
				},
			},
			customLibrary: "library2",
			expectedParam: "library2,library1",
		},
		"describe db default params error": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			customLibrary: "library2",
			expectedParam: "",
			expectedErr:   describeEngineParamsErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineParamsErr,
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			param, err := test.parameterGroupAdapter.buildCustomSharePreloadLibrariesParam(test.dbInstance, test.customLibrary)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if param != test.expectedParam {
				t.Fatalf("expected %s, got: %s", test.expectedParam, param)
			}
		})
	}
}

func TestGetCustomParameters(t *testing.T) {
	describeEngineParamsErr := errors.New("describe db engine params error")
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedParams        map[string]map[string]string
		expectedErr           error
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"enabled functions": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "1",
				},
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
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
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
				},
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
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
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
				},
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
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
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
					"binlog_format":                   "ROW",
				},
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				svc:      &mockRDSClient{},
				settings: config.Settings{},
			},
		},
		"enable PG cron": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libraries": "pg_cron",
				},
			},
			parameterGroupAdapter: &parameterGroupAdapter{
				settings: config.Settings{},
				svc: &mockRDSClient{
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
		"describe db default params error": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			expectedParams: nil,
			expectedErr:    describeEngineParamsErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				settings: config.Settings{},
				svc: &mockRDSClient{
					describeEngineDefaultParamsErr: describeEngineParamsErr,
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
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"no db version": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedPGroupFamily: "postgres12",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
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
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
			},
		},
		"RDS service returns error": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			expectedErr: serviceErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeEngVersionsErr: serviceErr,
				},
			},
		},
		"instance has parameter group family": {
			dbInstance: &RDSInstance{
				ParameterGroupFamily: "random-family",
			},
			expectedPGroupFamily: "random-family",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
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
		pGroupName            string
		expectedExists        bool
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"error, return false": {
			pGroupName:     "group1",
			expectedExists: false,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeDbParamsErr: dbParamsErr,
				},
			},
		},
		"no error, return true": {
			pGroupName:     "group2",
			expectedExists: true,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			exists := test.parameterGroupAdapter.checkIfParameterGroupExists(test.pGroupName)
			if exists != test.expectedExists {
				t.Fatalf("expected: %t, got: %t", test.expectedExists, exists)
			}
		})
	}
}

func TestCreateOrModifyCustomParameterGroupFunc(t *testing.T) {
	createDbParamGroupErr := errors.New("create DB params err")
	describeEngVersionsErr := errors.New("describe DB engine versions err")
	modifyDbParamGroupErr := errors.New("modify DB params err")

	testCases := map[string]struct {
		dbInstance            *RDSInstance
		expectedPGroupName    string
		expectedErr           error
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"error getting parameter group family": {
			dbInstance: &RDSInstance{
				Database: "foobar",
				DbType:   "postgres",
			},
			expectedPGroupName: "",
			expectedErr:        describeEngVersionsErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeDbParamsErr:    errors.New("describe DB params err"),
					describeEngVersionsErr: describeEngVersionsErr,
				},
			},
		},
		"error creating database parameter group": {
			dbInstance: &RDSInstance{
				Database:  "foobar",
				DbType:    "postgres",
				DbVersion: "12",
			},
			expectedPGroupName: "",
			expectedErr:        createDbParamGroupErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					describeDbParamsErr:   errors.New("describe DB params err"),
					createDbParamGroupErr: createDbParamGroupErr,
				},
			},
		},
		"error modifying database parameter group": {
			dbInstance: &RDSInstance{
				Database:  "foobar",
				DbType:    "postgres",
				DbVersion: "12",
			},
			expectedPGroupName: "",
			expectedErr:        modifyDbParamGroupErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					modifyDbParamGroupErr: modifyDbParamGroupErr,
				},
			},
		},
		"success": {
			dbInstance: &RDSInstance{
				Database:  "foobar",
				DbType:    "postgres",
				DbVersion: "12",
			},
			expectedPGroupName: "foobar",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc:                  &mockRDSClient{},
				parameterGroupPrefix: "",
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			pGroupName, err := test.parameterGroupAdapter.createOrModifyCustomParameterGroup(test.dbInstance, nil)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if pGroupName != test.expectedPGroupName {
				t.Errorf("expected parameter group name: %s, got: %s", test.expectedPGroupName, pGroupName)
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
		parameterGroupAdapter *parameterGroupAdapter
	}{
		"does not need custom params": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			expectedPGroupName: "",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{},
			},
		},
		"enable binary log format, success": {
			dbInstance: &RDSInstance{
				DbType:          "mysql",
				BinaryLogFormat: "ROW",
				Database:        "database1",
			},
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			expectedPGroupName: "prefix-database1",
			parameterGroupAdapter: &parameterGroupAdapter{
				svc:                  &mockRDSClient{},
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
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
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
				},
				parameterGroupPrefix: "prefix-",
			},
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			expectedPGroupName: "prefix-database2",
		},
		"needs custom params, error": {
			dbInstance: &RDSInstance{
				DbType:          "mysql",
				BinaryLogFormat: "ROW",
				Database:        "database1",
			},
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			expectedErr:        modifyDbParamGroupErr,
			parameterGroupAdapter: &parameterGroupAdapter{
				svc: &mockRDSClient{
					modifyDbParamGroupErr: modifyDbParamGroupErr,
				},
			},
			expectedPGroupName: "",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			pGroupName, err := test.parameterGroupAdapter.ProvisionCustomParameterGroupIfNecessary(
				test.dbInstance,
				test.dedicatedDBAdapter,
			)

			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if pGroupName != test.expectedPGroupName {
				t.Fatalf("unexpected group name: %s, expected: %s", pGroupName, test.expectedPGroupName)
			}
		})
	}
}
