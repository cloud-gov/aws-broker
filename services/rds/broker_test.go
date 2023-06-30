package rds

import (
	"reflect"
	"testing"

	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
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

func TestParseModifyOptionsFromRequest(t *testing.T) {
	testCases := map[string]struct {
		broker          *rdsBroker
		modifyRequest   request.Request
		expectedOptions Options
	}{
		"enable PG cron not specified": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(``),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				BackupRetentionPeriod: 0,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
			},
		},
		"enable PG cron true": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "enable_pg_cron": true }`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				BackupRetentionPeriod: 0,
				EnablePgCron:          aws.Bool(true),
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
			},
		},
		"enable PG cron false": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "enable_pg_cron": false }`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				BackupRetentionPeriod: 0,
				EnablePgCron:          aws.Bool(false),
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
			},
		},
		"rotate creds true": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "rotate_credentials": true }`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				BackupRetentionPeriod: 0,
				RotateCredentials:     aws.Bool(true),
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
			},
		},
		"rotate creds false": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "rotate_credentials": false }`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				BackupRetentionPeriod: 0,
				RotateCredentials:     aws.Bool(false),
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
			},
		},
		"rotate creds not specified": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{}`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				BackupRetentionPeriod: 0,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			options, err := test.broker.parseModifyOptionsFromRequest(test.modifyRequest)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !reflect.DeepEqual(test.expectedOptions, options) {
				t.Errorf("expected: %+v, got %+v", test.expectedOptions, options)
			}
		})
	}
}
