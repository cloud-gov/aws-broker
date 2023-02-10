package rds

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/18F/aws-broker/config"
	"github.com/aws/aws-sdk-go/aws"
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

func TestParseModifyOptions(t *testing.T) {
	testCases := map[string]struct {
		options            Options
		existingInstance   *RDSInstance
		expectedInstance   *RDSInstance
		expectResponseCode int
	}{
		"update allocated storage": {
			options: Options{
				AllocatedStorage: 20,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 10,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
		},
		"allocated storage option less than existing, does not update": {
			options: Options{
				AllocatedStorage: 10,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectResponseCode: http.StatusBadRequest,
		},
		"allocated storage empty, does not update": {
			options: Options{
				AllocatedStorage: 0,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectResponseCode: http.StatusBadRequest,
		},
		"update backup retention period": {
			options: Options{
				BackupRetentionPeriod: 20,
			},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 10,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
		},
		"does not update backup retention period": {
			options: Options{
				BackupRetentionPeriod: 0,
			},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
		},
		"update binary log format": {
			options: Options{
				BinaryLogFormat: "ROW",
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
			},
		},
		"enable PG cron": {
			options: Options{
				EnablePgCron: aws.Bool(true),
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
			},
		},
		"enable PG cron not specified": {
			options:          Options{},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			broker := &rdsBroker{}
			resp := broker.parseModifyOptions(test.options, test.existingInstance)
			if !reflect.DeepEqual(test.existingInstance, test.expectedInstance) {
				t.Fatalf("expected instance and updated instance were not equal")
			}
			if resp != nil && resp.GetStatusCode() != test.expectResponseCode {
				t.Fatalf("expected status code: %d, got %d", test.expectResponseCode, resp.GetStatusCode())
			}
		})
	}
}
