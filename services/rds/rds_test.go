package rds

import (
	"testing"

	"github.com/18F/aws-broker/config"
)

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
