package rds

import (
	"slices"
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

func TestValidateOracleOptions(t *testing.T) {
	boolPtr := func(b bool) *bool { return &b }
	testCases := map[string]struct {
		options     Options
		expectedErr bool
	}{
		"empty is valid":                       {options: Options{}, expectedErr: false},
		"allowed log exports":                  {options: Options{EnableCloudWatchLogGroupExports: []string{"alert", "audit", "listener"}}, expectedErr: false},
		"storage + backup ok":                  {options: Options{AllocatedStorage: 50, BackupRetentionPeriod: aws.Int64(30)}, expectedErr: false},
		"publicly_accessible rejected":         {options: Options{PubliclyAccessible: true}, expectedErr: true},
		"enable_functions rejected":            {options: Options{EnableFunctions: true}, expectedErr: true},
		"binary_log_format rejected":           {options: Options{BinaryLogFormat: "ROW"}, expectedErr: true},
		"enable_pg_cron rejected":              {options: Options{EnablePgCron: boolPtr(true)}, expectedErr: true},
		"version rejected":                     {options: Options{Version: "19.0.0.0"}, expectedErr: true},
		"allow_major_version_upgrade rejected": {options: Options{AllowMajorVersionUpgrade: boolPtr(true)}, expectedErr: true},
		"trace log export allowed":             {options: Options{EnableCloudWatchLogGroupExports: []string{"trace", "oemagent"}}, expectedErr: false},
		"bad log export rejected":              {options: Options{EnableCloudWatchLogGroupExports: []string{"slowquery"}}, expectedErr: true},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateOracleOptions(test.options)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

// TestOracleLogExportAllowlistMatchesBaseline is the #546 drift guard. The
// customer-facing log-export allowlist enforced by validateOracleOptions
// (validOracleLogExports) MUST be exactly the embedded baseline's `supported`
// set, and the baseline's `default_exports` (what the create path enables when
// the customer supplies none) MUST be a subset of that supported set. If a future
// refactor reintroduces a hand-maintained Go literal, or edits log_exports.yml
// inconsistently, this test fails instead of silently drifting.
func TestOracleLogExportAllowlistMatchesBaseline(t *testing.T) {
	baseline, err := loadOracleLogExports()
	if err != nil {
		t.Fatalf("loadOracleLogExports: %v", err)
	}

	// 1. The enforced allowlist is derived from — and equal to — the baseline
	//    `supported` set (single source of truth; no duplication).
	allow, err := validOracleLogExports()
	if err != nil {
		t.Fatalf("validOracleLogExports: %v", err)
	}
	if !slices.Equal(allow, baseline.Supported) {
		t.Errorf("validOracleLogExports() = %v, want baseline supported set %v (they must not drift)",
			allow, baseline.Supported)
	}

	// 2. default_exports ⊆ supported — you cannot default-enable an export the
	//    engine does not support (create path would advertise an invalid request).
	for _, d := range baseline.DefaultExports {
		if !slices.Contains(baseline.Supported, d) {
			t.Errorf("default export %q is not in the supported set %v", d, baseline.Supported)
		}
	}

	// 3. Every default export the create path enables must pass the same allowlist
	//    a customer request is validated against (i.e. the born-hardened default
	//    posture can never be self-rejected by validateOracleOptions).
	if err := validateOracleOptions(Options{
		EnableCloudWatchLogGroupExports: baseline.DefaultExports,
	}); err != nil {
		t.Errorf("default log exports %v rejected by validateOracleOptions: %v",
			baseline.DefaultExports, err)
	}
}
