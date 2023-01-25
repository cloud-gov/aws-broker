package rds

import (
	"testing"

	"github.com/18F/aws-broker/config"
)

func TestOptionsBinaryLogFormatValidation(t *testing.T) {
	testCases := map[string]struct {
		binaryLogFormat string
		settings        *config.Settings
		expectedErr     bool
	}{
		"invalid": {
			binaryLogFormat: "foo",
			settings:        &config.Settings{},
			expectedErr:     true,
		},
		"ROW": {
			binaryLogFormat: "ROW",
			settings:        &config.Settings{},
			expectedErr:     false,
		},
		"STATEMENT": {
			binaryLogFormat: "STATEMENT",
			settings:        &config.Settings{},
			expectedErr:     false,
		},
		"MIXED": {
			binaryLogFormat: "MIXED",
			settings:        &config.Settings{},
			expectedErr:     false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := Options{
				BinaryLogFormat: test.binaryLogFormat,
			}
			err := opts.Validate(test.settings)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}
