package rds

import (
	"reflect"
	"testing"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers"
	"github.com/aws/aws-sdk-go/aws"
)

func TestFormatDBName(t *testing.T) {
	i := RDSInstance{
		Database: "db" + helpers.RandStrNoCaps(15),
	}
	dbName1 := i.FormatDBName()
	dbName2 := i.FormatDBName()
	if dbName1 != dbName2 {
		t.Fatalf("database names should be the same")
	}
}

func TestModifyInstance(t *testing.T) {
	testCases := map[string]struct {
		options          Options
		existingInstance *RDSInstance
		expectedInstance *RDSInstance
		expectErr        bool
		plan             catalog.RDSPlan
		settings         *config.Settings
	}{
		"update allocated storage": {
			options: Options{
				AllocatedStorage: 20,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
		},
		"allocated storage option less than existing, does not update": {
			options: Options{
				AllocatedStorage: 20,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectErr: true,
			plan:      catalog.RDSPlan{},
			settings:  &config.Settings{},
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
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
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
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
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
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
		},
		"update binary log format": {
			options: Options{
				BinaryLogFormat: "ROW",
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
			},
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
		},
		"enable PG cron": {
			options: Options{
				EnablePgCron: aws.Bool(true),
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
			},
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
		},
		"enable PG cron not specified": {
			options:          Options{},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{},
			plan:             catalog.RDSPlan{},
			settings:         &config.Settings{},
		},
		"enable PG cron not specified on options, true on existing instance": {
			options: Options{},
			existingInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
			},
			expectedInstance: &RDSInstance{},
			plan:             catalog.RDSPlan{},
			settings:         &config.Settings{},
		},
		"set DB version from plan": {
			options:          Options{},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				DbVersion: "12",
			},
			plan: catalog.RDSPlan{
				DbVersion: "12",
			},
			settings: &config.Settings{},
		},
		"enable functions": {
			options: Options{
				EnableFunctions: true,
			},
			existingInstance: &RDSInstance{
				EnableFunctions: false,
			},
			expectedInstance: &RDSInstance{
				EnableFunctions: true,
			},
			plan:     catalog.RDSPlan{},
			settings: &config.Settings{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.existingInstance.modify(test.options, test.plan, test.settings)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !reflect.DeepEqual(test.existingInstance, test.expectedInstance) {
				t.Fatalf("expected instance: %+v, got instance: %+v", test.expectedInstance, test.existingInstance)
			}
		})
	}
}

func TestModifyInstanceRotateCredentials(t *testing.T) {
	testCases := map[string]struct {
		options                 Options
		plan                    catalog.RDSPlan
		settings                *config.Settings
		originalPassword        string
		originalSalt            string
		username                string
		shouldRotateCredentials bool
	}{
		"rotate credentials": {
			options: Options{
				RotateCredentials: aws.Bool(true),
			},
			plan: catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			originalPassword:        helpers.RandStr(20),
			originalSalt:            helpers.RandStr(10),
			username:                helpers.RandStr(10),
			shouldRotateCredentials: true,
		},
		"do not rotate credentials": {
			options: Options{
				RotateCredentials: aws.Bool(false),
			},
			plan: catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			originalPassword:        helpers.RandStr(20),
			originalSalt:            helpers.RandStr(10),
			username:                helpers.RandStr(10),
			shouldRotateCredentials: false,
		},
		"rotate credentials not specified": {
			options: Options{},
			plan:    catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			originalPassword:        helpers.RandStr(20),
			originalSalt:            helpers.RandStr(10),
			username:                helpers.RandStr(10),
			shouldRotateCredentials: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			existingInstance := &RDSInstance{
				Username:      test.username,
				ClearPassword: test.originalPassword,
				Salt:          test.originalSalt,
			}
			err := existingInstance.modify(test.options, test.plan, test.settings)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.shouldRotateCredentials && existingInstance.ClearPassword == test.originalPassword {
				t.Fatal("instance password should have been updated")
			}
			if test.shouldRotateCredentials && existingInstance.Salt == test.originalSalt {
				t.Fatal("instance salt should have been updated")
			}
		})
	}
}
