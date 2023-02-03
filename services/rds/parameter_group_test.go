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

var describeEngineCallNum int

func (m mockRDSClient) DescribeEngineDefaultParameters(*rds.DescribeEngineDefaultParametersInput) (*rds.DescribeEngineDefaultParametersOutput, error) {
	if m.describeEngineDefaultParamsErr != nil {
		return nil, m.describeEngineDefaultParamsErr
	}
	if m.describeEngineDefaultParamsResults != nil {
		res := m.describeEngineDefaultParamsResults[describeEngineCallNum]
		describeEngineCallNum++
		return res, nil
	}
	return nil, nil
}

func TestNeedCustomParameters(t *testing.T) {
	testCases := map[string]struct {
		dbInstance *RDSInstance
		settings   config.Settings
		expectedOk bool
	}{
		"default": {
			dbInstance: &RDSInstance{},
			settings:   config.Settings{},
			expectedOk: false,
		},
		"valid binary log format": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
			},
			settings:   config.Settings{},
			expectedOk: true,
		},
		"valid binary log format, wrong database type": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "psql",
			},
			settings:   config.Settings{},
			expectedOk: false,
		},
		"instance functions enabled, settings disabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: false,
			},
			expectedOk: false,
		},
		"instance functions disabled, settings enabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: false,
				DbType:          "mysql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: true,
			},
			expectedOk: false,
		},
		"instance functions enabled, settings enabled, wrong db type": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "psql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: true,
			},
			expectedOk: false,
		},
		"instance functions enabled, settings enabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: true,
			},
			expectedOk: true,
		},
		"enable PG cron": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
			},
			expectedOk: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			if needCustomParameters(test.dbInstance, test.settings) != test.expectedOk {
				t.Fatalf("should be %v", test.expectedOk)
			}
		})
	}
}

func TestGetDefaultEngineParameter(t *testing.T) {
	// describeEngineDefaultParamsErr := errors.New("describe db engine default params err")
	// describeEngVersionsErr := errors.New("describe eng versions err")
	testCases := map[string]struct {
		dbInstance                         *RDSInstance
		expectedParams                     map[string]map[string]string
		paramName                          string
		expectedParamValue                 string
		describeEngineDefaultParamsResults []*rds.DescribeEngineDefaultParametersOutput
		describeEngVersionsErr             error
		describeEngineDefaultParamsErr     error
		expectedErr                        error
	}{
		// "no default param value": {
		// 	dbInstance: &RDSInstance{
		// 		EnablePgCron: true,
		// 		DbType:       "postgres",
		// 		DbVersion:    "12",
		// 	},
		// 	paramName: "shared_preload_libraries",
		// 	expectedParams: map[string]map[string]string{
		// 		"postgres": {
		// 			"shared_preload_libaries": "pg-cron",
		// 		},
		// 	},
		// 	describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
		// 		{
		// 			EngineDefaults: &rds.EngineDefaults{
		// 				Parameters: []*rds.Parameter{},
		// 			},
		// 		},
		// 	},
		// 	expectedParamValue: "",
		// },
		// "default param value": {
		// 	dbInstance: &RDSInstance{
		// 		EnablePgCron: true,
		// 		DbType:       "postgres",
		// 		DbVersion:    "12",
		// 	},
		// 	paramName: "shared_preload_libraries",
		// 	expectedParams: map[string]map[string]string{
		// 		"postgres": {
		// 			"shared_preload_libaries": "pg-cron",
		// 		},
		// 	},
		// 	describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
		// 		{
		// 			EngineDefaults: &rds.EngineDefaults{
		// 				Parameters: []*rds.Parameter{
		// 					{
		// 						ParameterName:  aws.String("shared_preload_libraries"),
		// 						ParameterValue: aws.String("random-library"),
		// 					},
		// 				},
		// 			},
		// 		},
		// 	},
		// 	expectedParamValue: "random-library",
		// },
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
			expectedParamValue: "a-library,b-library",
		},
		// "describe db engine params error": {
		// 	dbInstance: &RDSInstance{
		// 		EnablePgCron: true,
		// 		DbType:       "postgres",
		// 		DbVersion:    "12",
		// 	},
		// 	paramName: "shared_preload_libraries",
		// 	expectedParams: map[string]map[string]string{
		// 		"postgres": {
		// 			"shared_preload_libaries": "pg-cron",
		// 		},
		// 	},
		// 	describeEngineDefaultParamsErr: describeEngineDefaultParamsErr,
		// 	expectedErr:                    describeEngineDefaultParamsErr,
		// 	expectedParamValue:             "",
		// },
		// "describe db engine versions error": {
		// 	dbInstance: &RDSInstance{
		// 		EnablePgCron: true,
		// 		DbType:       "postgres",
		// 	},
		// 	paramName: "shared_preload_libraries",
		// 	expectedParams: map[string]map[string]string{
		// 		"postgres": {
		// 			"shared_preload_libaries": "pg-cron",
		// 		},
		// 	},
		// 	describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
		// 		{
		// 			EngineDefaults: &rds.EngineDefaults{
		// 				Parameters: []*rds.Parameter{},
		// 			},
		// 		},
		// 	},
		// 	describeEngVersionsErr: describeEngVersionsErr,
		// 	expectedErr:            describeEngVersionsErr,
		// 	expectedParamValue:     "",
		// },
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			describeEngineCallNum = 0
			paramValue, err := getDefaultEngineParameter(test.paramName, test.dbInstance, &mockRDSClient{
				describeEngineDefaultParamsResults: test.describeEngineDefaultParamsResults,
				describeEngineDefaultParamsErr:     test.describeEngineDefaultParamsErr,
				describeEngVersionsErr:             test.describeEngVersionsErr,
			})
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if paramValue != test.expectedParamValue {
				t.Errorf("expected: %s, got: %s", test.expectedParamValue, paramValue)
			}
		})
	}
}

func TestBuildCustomSharePreloadLibrariesParam(t *testing.T) {
	describeEngineParamsErr := errors.New("describe engine params error")
	testCases := map[string]struct {
		dbInstance                         *RDSInstance
		describeEngineDefaultParamsResults []*rds.DescribeEngineDefaultParametersOutput
		customLibrary                      string
		expectedParam                      string
		expectedErr                        error
		describeEngineDefaultParamsErr     error
	}{
		"no default param value": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
				{
					EngineDefaults: &rds.EngineDefaults{
						Parameters: []*rds.Parameter{},
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
			customLibrary: "library2",
			expectedParam: "library2,library1",
		},
		"describe db default params error": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			customLibrary:                  "library2",
			expectedParam:                  "",
			expectedErr:                    describeEngineParamsErr,
			describeEngineDefaultParamsErr: describeEngineParamsErr,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			describeEngineCallNum = 0
			param, err := buildCustomSharePreloadLibrariesParam(test.dbInstance, test.customLibrary, &mockRDSClient{
				describeEngineDefaultParamsResults: test.describeEngineDefaultParamsResults,
				describeEngineDefaultParamsErr:     test.describeEngineDefaultParamsErr,
			})
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && !errors.Is(err, test.expectedErr) {
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
		dbInstance                         *RDSInstance
		settings                           config.Settings
		expectedParams                     map[string]map[string]string
		describeEngineDefaultParamsResults []*rds.DescribeEngineDefaultParametersOutput
		expectedErr                        error
		describeEngineDefaultParamsErr     error
	}{
		"enabled functions": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: true,
			},
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "1",
				},
			},
		},
		"instance functions disabled, settings enabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: false,
				DbType:          "mysql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: true,
			},
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
				},
			},
		},
		"instance functions enabled, settings disabled": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
				DbType:          "mysql",
			},
			settings: config.Settings{
				EnableFunctionsFeature: false,
			},
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
				},
			},
		},
		"binary log format": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
			},
			settings: config.Settings{},
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
					"binlog_format":                   "ROW",
				},
			},
		},
		"enable PG cron": {
			dbInstance: &RDSInstance{
				EnablePgCron: true,
				DbType:       "postgres",
				DbVersion:    "12",
			},
			settings: config.Settings{},
			expectedParams: map[string]map[string]string{
				"postgres": {
					"shared_preload_libraries": "pg_cron",
				},
			},
			describeEngineDefaultParamsResults: []*rds.DescribeEngineDefaultParametersOutput{
				{
					EngineDefaults: &rds.EngineDefaults{
						Parameters: []*rds.Parameter{},
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
			settings:                       config.Settings{},
			expectedParams:                 nil,
			expectedErr:                    describeEngineParamsErr,
			describeEngineDefaultParamsErr: describeEngineParamsErr,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			describeEngineCallNum = 0
			params, err := getCustomParameters(test.dbInstance, test.settings, &mockRDSClient{
				describeEngineDefaultParamsResults: test.describeEngineDefaultParamsResults,
				describeEngineDefaultParamsErr:     test.describeEngineDefaultParamsErr,
			})
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && !errors.Is(err, test.expectedErr) {
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
		dbInstance             *RDSInstance
		dbEngineVersions       []*rds.DBEngineVersion
		describeEngVersionsErr error
		expectedErr            error
		expectedPGroupFamily   string
	}{
		"no db version": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			dbEngineVersions: []*rds.DBEngineVersion{
				{
					DBParameterGroupFamily: aws.String("postgres12"),
				},
			},
			expectedPGroupFamily: "postgres12",
		},
		"has db version": {
			dbInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "13",
			},
			expectedPGroupFamily: "postgres13",
		},
		"RDS service returns error": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			describeEngVersionsErr: serviceErr,
			expectedErr:            serviceErr,
		},
		"instance has parameter group family": {
			dbInstance: &RDSInstance{
				ParameterGroupFamily: "random-family",
			},
			expectedPGroupFamily: "random-family",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := getParameterGroupFamily(test.dbInstance, mockRDSClient{
				dbEngineVersions:       test.dbEngineVersions,
				describeEngVersionsErr: test.describeEngVersionsErr,
			})
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && err != test.expectedErr {
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
		pGroupName          string
		describeDbParamsErr error
		expectedExists      bool
	}{
		"error, return false": {
			describeDbParamsErr: dbParamsErr,
			pGroupName:          "group1",
			expectedExists:      false,
		},
		"no error, return true": {
			pGroupName:     "group2",
			expectedExists: true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			exists := checkIfParameterGroupExists(test.pGroupName, mockRDSClient{
				describeDbParamsErr: test.describeDbParamsErr,
			})
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
		dbInstance             *RDSInstance
		describeDbParamsErr    error
		describeEngVersionsErr error
		createDbParamGroupErr  error
		modifyDbParamGroupErr  error
		expectedPGroupName     string
		pGroupPrefix           string
		expectedErr            error
	}{
		"error getting parameter group family": {
			dbInstance: &RDSInstance{
				Database: "foobar",
				DbType:   "postgres",
			},
			describeDbParamsErr:    errors.New("describe DB params err"),
			describeEngVersionsErr: describeEngVersionsErr,
			expectedPGroupName:     "",
			expectedErr:            describeEngVersionsErr,
		},
		"error creating database parameter group": {
			dbInstance: &RDSInstance{
				Database:  "foobar",
				DbType:    "postgres",
				DbVersion: "12",
			},
			describeDbParamsErr:   errors.New("describe DB params err"),
			createDbParamGroupErr: createDbParamGroupErr,
			expectedPGroupName:    "",
			expectedErr:           createDbParamGroupErr,
		},
		"error modifying database parameter group": {
			dbInstance: &RDSInstance{
				Database:  "foobar",
				DbType:    "postgres",
				DbVersion: "12",
			},
			modifyDbParamGroupErr: modifyDbParamGroupErr,
			expectedPGroupName:    "",
			expectedErr:           modifyDbParamGroupErr,
		},
		"success": {
			dbInstance: &RDSInstance{
				Database:  "foobar",
				DbType:    "postgres",
				DbVersion: "12",
			},
			pGroupPrefix:       "",
			expectedPGroupName: "foobar",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			pGroupPrefix = test.pGroupPrefix
			pGroupName, err := createOrModifyCustomParameterGroup(test.dbInstance, nil, &mockRDSClient{
				describeDbParamsErr:    test.describeDbParamsErr,
				createDbParamGroupErr:  test.createDbParamGroupErr,
				describeEngVersionsErr: test.describeEngVersionsErr,
				modifyDbParamGroupErr:  test.modifyDbParamGroupErr,
			})
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && !errors.Is(err, test.expectedErr) {
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
		pGroupPrefix          string
		modifyDbParamGroupErr error
		expectedPGroupName    string
		expectedErr           error
		dedicatedDBAdapter    *dedicatedDBAdapter
		mockRDSClient         *mockRDSClient
	}{
		"does not need custom params": {
			dbInstance: &RDSInstance{
				DbType: "postgres",
			},
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			expectedPGroupName: "",
			mockRDSClient:      &mockRDSClient{},
		},
		"enable binary log format, success": {
			dbInstance: &RDSInstance{
				DbType:          "mysql",
				BinaryLogFormat: "ROW",
				Database:        "database1",
			},
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			pGroupPrefix:       "prefix-",
			expectedPGroupName: "prefix-database1",
			mockRDSClient:      &mockRDSClient{},
		},
		"enable PG cron, success": {
			dbInstance: &RDSInstance{
				DbType:       "postgres",
				DbVersion:    "12",
				EnablePgCron: true,
				Database:     "database2",
			},
			mockRDSClient: &mockRDSClient{
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
			dedicatedDBAdapter: &dedicatedDBAdapter{},
			pGroupPrefix:       "prefix-",
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
			mockRDSClient: &mockRDSClient{
				modifyDbParamGroupErr: modifyDbParamGroupErr,
			},
			expectedPGroupName: "",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			pGroupPrefix = test.pGroupPrefix

			p := &parameterGroupAdapter{}
			pGroupName, err := p.provisionCustomParameterGroupIfNecessary(
				test.dbInstance,
				test.dedicatedDBAdapter,
				test.mockRDSClient,
			)

			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && !errors.Is(err, test.expectedErr) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if pGroupName != test.expectedPGroupName {
				t.Fatalf("unexpected group name: %s, expected: %s", pGroupName, test.expectedPGroupName)
			}
		})
	}
}
