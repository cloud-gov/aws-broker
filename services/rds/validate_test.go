package rds

import (
	"testing"

	"github.com/18F/aws-broker/config"
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

func TestValidateRetentionPeriod(t *testing.T) {
	testCases := map[string]struct {
		retentionPeriod int
		expectedErr     bool
	}{
		"should not allow retention period of 0": {
			retentionPeriod: 0,
			expectedErr:     true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := &Options{
				BackupRetentionPeriod: int64(test.retentionPeriod),
			}
			err := opts.Validate(&config.Settings{
				MinBackupRetention: 14,
			})
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}
