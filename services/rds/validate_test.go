package rds

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
)

func TestBinaryLogFormatValidation(t *testing.T) {
	testCases := map[string]struct {
		binaryLogFormat string
		expectedErr     bool
	}{
		"invalid": {
			binaryLogFormat: "foo",
			expectedErr:     true,
		},
		"empty": {
			binaryLogFormat: "",
			expectedErr:     false,
		},
		"ROW": {
			binaryLogFormat: "ROW",
			expectedErr:     false,
		},
		"STATEMENT": {
			binaryLogFormat: "STATEMENT",
			expectedErr:     false,
		},
		"MIXED": {
			binaryLogFormat: "MIXED",
			expectedErr:     false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateBinaryLogFormat(test.binaryLogFormat)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestValidateStorageType(t *testing.T) {
	testCases := map[string]struct {
		storageType string
		expectedErr bool
	}{
		"invalid": {
			storageType: "io1",
			expectedErr: true,
		},
		"empty": {
			storageType: "",
			expectedErr: false,
		},
		"gp2": {
			storageType: "gp2",
			expectedErr: true,
		},
		"gp3": {
			storageType: "gp3",
			expectedErr: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateStorageType(test.storageType)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestValidateLongQueryTime(t *testing.T) {
	testCases := map[string]struct {
		value       *float64
		expectedErr bool
	}{
		"nil": {
			value:       nil,
			expectedErr: false,
		},
		"valid float": {
			value:       aws.Float64(0.5),
			expectedErr: false,
		},
		"valid int": {
			value:       aws.Float64(10),
			expectedErr: false,
		},
		"negative": {
			value:       aws.Float64(-1),
			expectedErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateLongQueryTime(test.value)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestValidatePgQueryLogging(t *testing.T) {
	testCases := map[string]struct {
		value       *PgQueryLoggingOptions
		expectedErr bool
	}{
		"nil": {
			value:       nil,
			expectedErr: false,
		},
		"empty": {
			value:       &PgQueryLoggingOptions{},
			expectedErr: false,
		},
		"valid log_min_duration_statement": {
			value:       &PgQueryLoggingOptions{LogMinDurationStatement: aws.Int64(500)},
			expectedErr: false,
		},
		"invalid log_min_duration_statement": {
			value:       &PgQueryLoggingOptions{LogMinDurationStatement: aws.Int64(-2)},
			expectedErr: true,
		},
		"valid log_min_duration_sample": {
			value:       &PgQueryLoggingOptions{LogMinDurationSample: aws.Int64(100)},
			expectedErr: false,
		},
		"invalid log_min_duration_sample": {
			value:       &PgQueryLoggingOptions{LogMinDurationSample: aws.Int64(-2)},
			expectedErr: true,
		},
		"valid log_statement ddl": {
			value:       &PgQueryLoggingOptions{LogStatement: aws.String("ddl")},
			expectedErr: false,
		},
		"invalid log_statement": {
			value:       &PgQueryLoggingOptions{LogStatement: aws.String("INVALID")},
			expectedErr: true,
		},
		"valid log_statement_sample_rate": {
			value:       &PgQueryLoggingOptions{LogStatementSampleRate: aws.Float64(0.5)},
			expectedErr: false,
		},
		"invalid log_statement_sample_rate": {
			value:       &PgQueryLoggingOptions{LogStatementSampleRate: aws.Float64(1.1)},
			expectedErr: true,
		},
		"valid log_connections as true": {
			value: &PgQueryLoggingOptions{LogConnections: aws.String("true")},
		},
		"valid log_connections as false": {
			value: &PgQueryLoggingOptions{LogConnections: aws.String("false")},
		},
		"valid log_connections": {
			value: &PgQueryLoggingOptions{LogConnections: aws.String("all")},
		},
		"invalid log_connections": {
			value:       &PgQueryLoggingOptions{LogConnections: aws.String("foo")},
			expectedErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validatePgQueryLogging(test.value)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestValidateRetentionPeriod(t *testing.T) {
	testCases := map[string]struct {
		retentionPeriod int64
		expectedErr     bool
		settings        *config.Settings
	}{
		// 0 was a special case in the previous buggy code, so should be
		// left as a standalone test case. A retention period value of 0
		// will disable backups on a database.
		"should not allow retention period of 0": {
			retentionPeriod: 0,
			expectedErr:     true,
			settings: &config.Settings{
				MinBackupRetention: 14,
			},
		},
		"should not allow retention period of less than the minimum": {
			retentionPeriod: 5,
			expectedErr:     true,
			settings: &config.Settings{
				MinBackupRetention: 14,
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := &Options{
				BackupRetentionPeriod: aws.Int64(test.retentionPeriod),
			}
			err := opts.Validate(test.settings, "")
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestValidateCloudwatchLogGroupExports(t *testing.T) {
	testCases := map[string]struct {
		dbType      string
		logTypes    []string
		expectedErr bool
	}{
		"nil logTypes": {
			dbType:      "postgres",
			logTypes:    nil,
			expectedErr: false,
		},
		"empty logTypes": {
			dbType:      "postgres",
			logTypes:    []string{},
			expectedErr: false,
		},
		"postgres valid single": {
			dbType:      "postgres",
			logTypes:    []string{"postgresql"},
			expectedErr: false,
		},
		"postgres valid both": {
			dbType:      "postgres",
			logTypes:    []string{"postgresql", "upgrade"},
			expectedErr: false,
		},
		"postgres invalid": {
			dbType:      "postgres",
			logTypes:    []string{"foo"},
			expectedErr: true,
		},
		"postgres mysql log type rejected": {
			dbType:      "postgres",
			logTypes:    []string{"general"},
			expectedErr: true,
		},
		"postgres one valid one invalid": {
			dbType:      "postgres",
			logTypes:    []string{"postgresql", "foo"},
			expectedErr: true,
		},
		"mysql valid all": {
			dbType:      "mysql",
			logTypes:    []string{"audit", "error", "general", "slowquery"},
			expectedErr: false,
		},
		"mysql invalid": {
			dbType:      "mysql",
			logTypes:    []string{"postgresql"},
			expectedErr: true,
		},
		"oracle-se2 valid": {
			dbType:      "oracle-se2",
			logTypes:    []string{"alert", "audit", "listener", "trace"},
			expectedErr: false,
		},
		"oracle-se2 invalid": {
			dbType:      "oracle-se2",
			logTypes:    []string{"general"},
			expectedErr: true,
		},
		"unknown dbType is not validated": {
			dbType:      "not-a-real-engine",
			logTypes:    []string{"anything"},
			expectedErr: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateCloudwatchLogGroupExports(test.dbType, test.logTypes)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestOracleCloudwatchLogExportsSupportedSetIsPinned(t *testing.T) {
	wantSupported := []string{"alert", "audit", "listener", "trace"}

	gotSupported := validCloudwatchLogTypesByDbType["oracle-se2"]
	if !slices.Equal(gotSupported, wantSupported) {
		t.Errorf(
			"validCloudwatchLogTypesByDbType[\"oracle-se2\"] = %v, want %v (a change here must be deliberate, not an accidental drift)",
			gotSupported, wantSupported,
		)
	}
	if err := validateCloudwatchLogGroupExports("oracle-se2", wantSupported); err != nil {
		t.Errorf("supported oracle-se2 log exports %v rejected by validateCloudwatchLogGroupExports: %v", wantSupported, err)
	}
}

func TestOracleCloudwatchLogExportsFlowUnchangedThroughCreatePath(t *testing.T) {
	supported := validCloudwatchLogTypesByDbType["oracle-se2"]

	worker := &CreateWorker{
		settings:          &config.Settings{},
		rds:               &mockRDSClient{},
		optionGroupClient: &mockOptionGroupClient{},
		parameterGroupClient: &mockParameterGroupClient{
			rds: &mockRDSClient{},
		},
	}

	i := &RDSInstance{
		Database:                         "db1",
		DbType:                           "oracle-se2",
		credentialUtils:                  &RDSCredentialUtils{},
		Username:                         "fake-user",
		EnabledCloudwatchLogGroupExports: supported,
	}

	params, err := worker.prepareCreateDbInput(i, &catalog.RDSPlan{}, "fake-password")
	if err != nil {
		t.Fatalf("prepareCreateDbInput: unexpected error: %s", err)
	}

	for _, logType := range supported {
		if !slices.Contains(params.EnableCloudwatchLogsExports, logType) {
			t.Errorf(
				"supported oracle-se2 log export %q did not flow through to CreateDBInstanceInput.EnableCloudwatchLogsExports, got %v",
				logType, params.EnableCloudwatchLogsExports,
			)
		}
	}
	for _, logType := range params.EnableCloudwatchLogsExports {
		if !slices.Contains(supported, logType) {
			t.Errorf(
				"CreateDBInstanceInput.EnableCloudwatchLogsExports contains %q, which is not a member of the oracle-se2 supported set %v",
				logType, supported,
			)
		}
	}
}
