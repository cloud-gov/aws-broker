package rds

import (
	"errors"
	"reflect"
	"testing"

	"github.com/18F/aws-broker/config"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
)

type mockRDSClient struct {
	rdsiface.RDSAPI
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
