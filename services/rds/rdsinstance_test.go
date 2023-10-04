package rds

import (
	"reflect"
	"testing"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/go-test/deep"
)

type MockDbUtils struct {
	mockFormattedDbName   string
	mockDbName            string
	mockUsername          string
	mockSalt              string
	mockEncryptedPassword string
	mockClearPassword     string
}

func (m *MockDbUtils) FormatDBName(string, string) string {
	return m.mockFormattedDbName
}

func (m *MockDbUtils) getCredentials(i *RDSInstance, password string) (map[string]string, error) {
	return nil, nil
}

func (m *MockDbUtils) generateCredentials(settings *config.Settings) (string, string, string, error) {
	return m.mockSalt, m.mockEncryptedPassword, m.mockClearPassword, nil
}

func (m *MockDbUtils) generatePassword(salt string, password string, key string) (string, string, error) {
	return m.mockEncryptedPassword, m.mockClearPassword, nil
}

func (m *MockDbUtils) getPassword(salt string, password string, key string) (string, error) {
	return m.mockClearPassword, nil
}

func (m *MockDbUtils) generateDatabaseName(settings *config.Settings) string {
	return m.mockDbName
}

func (m *MockDbUtils) buildUsername() string {
	return m.mockUsername
}

func TestFormatDBName(t *testing.T) {
	i := &RDSInstance{
		dbUtils: &MockDbUtils{
			mockFormattedDbName: "foobar",
		},
		Database: "db" + helpers.RandStrNoCaps(15),
	}
	dbName1 := i.FormatDBName()
	if dbName1 != "foobar" {
		t.Fatalf("database name should be foobar")
	}
	dbName2 := i.FormatDBName()
	if dbName1 != dbName2 {
		t.Fatalf("database names should be the same")
	}
}

func TestInit(t *testing.T) {
	testCases := map[string]struct {
		options          Options
		rdsInstance      *RDSInstance
		expectedInstance *RDSInstance
		expectErr        bool
		plan             catalog.RDSPlan
		settings         *config.Settings
		uuid             string
		orgGUID          string
		spaceGUID        string
		serviceID        string
		expectedErr      error
		testDbName       string
	}{
		"sets expected properties with plan defaults": {
			options: Options{},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				Adapter:               "adapter-1",
				DbType:                "postgres",
				DbVersion:             "15",
				BackupRetentionPeriod: 14,
				SubnetGroup:           "subnet-1",
				SecurityGroup:         "security-group-1",
				LicenseModel:          "license-model",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			uuid:      "uuid-1",
			orgGUID:   "org-1",
			spaceGUID: "space-1",
			serviceID: "service-1",
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{
					mockFormattedDbName:   "test-db",
					mockDbName:            "db",
					mockUsername:          "fake-user",
					mockSalt:              "salt",
					mockEncryptedPassword: "encrypted-pw",
					mockClearPassword:     "clear-pw",
				},
			},
			expectedInstance: &RDSInstance{
				Database: "db",
				Username: "fake-user",
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID:        "service-1",
						PlanID:           "plan-1",
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
					},
				},
				Adapter:               "adapter-1",
				DbType:                "postgres",
				DbVersion:             "15",
				BackupRetentionPeriod: 14,
				Tags: map[string]string{
					"Instance GUID":     "uuid-1",
					"Organization GUID": "org-1",
					"Space GUID":        "space-1",
					"Plan GUID":         "plan-1",
					"Service GUID":      "service-1",
				},
				StorageType:        "gp3",
				AllocatedStorage:   20,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				LicenseModel:       "license-model",
				DbSubnetGroup:      "subnet-1",
				SecGroup:           "security-group-1",
				Salt:               "salt",
				Password:           "encrypted-pw",
				ClearPassword:      "clear-pw",
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.rdsInstance.init(
				test.uuid,
				test.orgGUID,
				test.spaceGUID,
				test.serviceID,
				test.plan,
				test.options,
				test.settings,
			)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if diff := deep.Equal(test.rdsInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
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
		expectedErr      error
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
				AllocatedStorage: 10,
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
		"gp3 fails for allocated storage < 20": {
			options: Options{
				StorageType: "gp3",
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 10,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 10,
			},
			plan:      catalog.RDSPlan{},
			settings:  &config.Settings{},
			expectErr: true,
		},
		"gp3 upgrade succeeds": {
			options: Options{
				StorageType: "gp3",
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
				StorageType:      "gp3",
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
				StorageType:      "gp3",
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
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
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
