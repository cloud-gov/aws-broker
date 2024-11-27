package rds

import (
	"reflect"
	"testing"

	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/aws/aws-sdk-go/aws"
)

func TestValidate(t *testing.T) {
	testCases := map[string]struct {
		options     Options
		settings    *config.Settings
		expectedErr bool
	}{
		"invalid binary log format": {
			options: Options{
				BinaryLogFormat: "foo",
			},
			settings:    &config.Settings{},
			expectedErr: true,
		},
		"MIXED binary log format": {
			options: Options{
				BinaryLogFormat: "MIXED",
			},
			settings:    &config.Settings{},
			expectedErr: false,
		},
		"accepted storage type": {
			options: Options{
				StorageType: "gp3",
			},
			settings:    &config.Settings{},
			expectedErr: false,
		},
		"invalid storage type": {
			options: Options{
				StorageType: "io1",
			},
			settings:    &config.Settings{},
			expectedErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.options.Validate(test.settings)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestParseModifyOptionsFromRequest(t *testing.T) {
	testCases := map[string]struct {
		broker          *rdsBroker
		modifyRequest   request.Request
		expectedOptions Options
		expectErr       bool
	}{
		"enable PG cron not specified": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(``),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
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
				AllocatedStorage:   0,
				EnablePgCron:       aws.Bool(true),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
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
				AllocatedStorage:   0,
				EnablePgCron:       aws.Bool(false),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
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
				AllocatedStorage:   0,
				RotateCredentials:  aws.Bool(true),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
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
				AllocatedStorage:   0,
				RotateCredentials:  aws.Bool(false),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
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
				AllocatedStorage:   0,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"backup retention period less than minimum is rejected": {
			broker: &rdsBroker{
				settings: &config.Settings{
					MinBackupRetention: 14,
				},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{"backup_retention_period": 0}`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
				BackupRetentionPeriod: aws.Int64(0),
			},
			expectErr: true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			options, err := test.broker.parseModifyOptionsFromRequest(test.modifyRequest)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !reflect.DeepEqual(test.expectedOptions, options) {
				t.Errorf("expected: %+v, got %+v", test.expectedOptions, options)
			}
		})
	}
}
