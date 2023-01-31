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

	dbEngineVersions       []*rds.DBEngineVersion
	describeEngVersionsErr error
	describeDbParamsErr    error
	createDbParamGroupErr  error
	modifyDbParamGroupErr  error
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
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			if needCustomParameters(test.dbInstance, test.settings) != test.expectedOk {
				t.Fatalf("should be %v", test.expectedOk)
			}
		})
	}
}

func TestGetCustomParameters(t *testing.T) {
	testCases := map[string]struct {
		dbInstance     *RDSInstance
		settings       config.Settings
		expectedParams map[string]map[string]string
	}{
		"enabled functions": {
			dbInstance: &RDSInstance{
				EnableFunctions: true,
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
			},
			settings: config.Settings{},
			expectedParams: map[string]map[string]string{
				"mysql": {
					"log_bin_trust_function_creators": "0",
					"binlog_format":                   "ROW",
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params := getCustomParameters(test.dbInstance, test.settings)
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
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			pGroupFamily, err := getParameterGroupFamily(test.dbInstance, mockRDSClient{
				dbEngineVersions:       test.dbEngineVersions,
				describeEngVersionsErr: test.describeEngVersionsErr,
			})
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && err != test.expectedErr {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if pGroupFamily != test.expectedPGroupFamily {
				t.Fatalf("expected parameter group family: %s, got: %s", test.expectedPGroupFamily, pGroupFamily)
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
			pGroupName, err := createOrModifyCustomParameterGroupFunc(test.dbInstance, nil, &mockRDSClient{
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
	p := &parameterGroupAdapter{}
	i := &RDSInstance{}
	d := &dedicatedDBAdapter{}
	svc := &mockRDSClient{}

	createModifyErr := errors.New("create/modify error")

	testCases := map[string]struct {
		customParams         map[string]map[string]string
		needCustomParameters bool
		pGroupName           string
		createOrModifyErr    error
		expectedPGroupName   string
		expectedErr          error
	}{
		"does not need custom params": {
			needCustomParameters: false,
		},
		"needs custom params, success": {
			needCustomParameters: true,
			pGroupName:           "group1",
			expectedPGroupName:   "group1",
		},
		"needs custom params, error": {
			needCustomParameters: true,
			createOrModifyErr:    createModifyErr,
			expectedErr:          createModifyErr,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			needCustomParameters = func(i *RDSInstance, s config.Settings) bool {
				return test.needCustomParameters
			}

			createOrModifyCustomParameterGroup = func(
				i *RDSInstance,
				customparams map[string]map[string]string,
				svc rdsiface.RDSAPI,
			) (string, error) {
				if test.createOrModifyErr != nil {
					return "", test.createOrModifyErr
				}
				return test.pGroupName, nil
			}

			pGroupName, err := p.provisionCustomParameterGroupIfNecessary(i, d, svc)
			if test.expectedErr == nil && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr != nil && err != test.expectedErr {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if pGroupName != test.expectedPGroupName {
				t.Fatalf("unexpected group name: %s, expected: %s", pGroupName, test.expectedPGroupName)
			}
		})
	}
}
