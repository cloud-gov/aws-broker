package rds

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
			err := opts.Validate(test.settings)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}
