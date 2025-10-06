package rds

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/go-test/deep"
)

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
		tags             map[string]string
	}{
		"sets expected properties": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(21),
			},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				Adapter:          "adapter-1",
				DbType:           "postgres",
				DbVersion:        "15",
				SubnetGroup:      "subnet-1",
				SecurityGroup:    "security-group-1",
				LicenseModel:     "license-model",
				StorageType:      "gp3",
				AllocatedStorage: 20,
				Tags:             map[string]string{},
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
				BackupRetentionPeriod: 21,
				Tags:                  map[string]string{},
				StorageType:           "gp3",
				AllocatedStorage:      20,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				LicenseModel:          "license-model",
				DbSubnetGroup:         "subnet-1",
				SecGroup:              "security-group-1",
				Salt:                  "salt",
				Password:              "encrypted-pw",
				ClearPassword:         "clear-pw",
			},
		},
		"MySQL sets db version from plan": {
			options: Options{},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				DbType:                "mysql",
				DbVersion:             "8.0",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 14,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{},
			},
			uuid:      "uuid-1",
			orgGUID:   "org-1",
			spaceGUID: "space-1",
			serviceID: "service-1",
			expectedInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID:        "service-1",
						PlanID:           "plan-1",
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
					},
				},
				DbType:                "mysql",
				DbVersion:             "8.0",
				BackupRetentionPeriod: 14,
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
		},
		"MySQL sets db version from options": {
			options: Options{
				Version: "9.0",
			},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				DbType:                "mysql",
				DbVersion:             "8.0",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 14,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{},
			},
			uuid:      "uuid-1",
			orgGUID:   "org-1",
			spaceGUID: "space-1",
			serviceID: "service-1",
			expectedInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID:        "service-1",
						PlanID:           "plan-1",
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
					},
				},
				DbType:                "mysql",
				DbVersion:             "9.0",
				BackupRetentionPeriod: 14,
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
		},
		"PostgreSQL sets db version from options": {
			options: Options{
				Version: "15",
			},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				DbType:                "postgres",
				DbVersion:             "12",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 14,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{},
			},
			uuid:      "uuid-1",
			orgGUID:   "org-1",
			spaceGUID: "space-1",
			serviceID: "service-1",
			expectedInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID:        "service-1",
						PlanID:           "plan-1",
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
					},
				},
				DbType:                "postgres",
				DbVersion:             "15",
				BackupRetentionPeriod: 14,
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
		},
		"sets backup retention period from plan": {
			options: Options{},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				DbType:                "postgres",
				DbVersion:             "15",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 23,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{},
			},
			uuid:      "uuid-1",
			orgGUID:   "org-1",
			spaceGUID: "space-1",
			serviceID: "service-1",
			expectedInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID:        "service-1",
						PlanID:           "plan-1",
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
					},
				},
				DbType:                "postgres",
				DbVersion:             "15",
				BackupRetentionPeriod: 23,
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
		},
		"merges plan and instance tags": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(14),
			},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				Adapter:          "adapter-1",
				DbType:           "postgres",
				DbVersion:        "15",
				SubnetGroup:      "subnet-1",
				SecurityGroup:    "security-group-1",
				LicenseModel:     "license-model",
				StorageType:      "gp3",
				AllocatedStorage: 20,
				Tags: map[string]string{
					"plan-tag": "random-value",
				},
			},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			uuid:      "uuid-1",
			orgGUID:   "org-1",
			spaceGUID: "space-1",
			serviceID: "service-1",
			tags: map[string]string{
				"foo": "bar",
			},
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{
					mockFormattedDbName:   "test-db",
					mockDbName:            "db",
					mockUsername:          "fake-user",
					mockSalt:              "salt",
					mockEncryptedPassword: "encrypted-pw",
					mockClearPassword:     "clear-pw",
				},
				Tags: map[string]string{
					"foo": "bar",
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
					"plan-tag": "random-value",
					"foo":      "bar",
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
		"plan has read replica enabled": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(21),
			},
			plan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-1",
				},
				Adapter:          "adapter-1",
				DbType:           "postgres",
				DbVersion:        "15",
				SubnetGroup:      "subnet-1",
				SecurityGroup:    "security-group-1",
				LicenseModel:     "license-model",
				StorageType:      "gp3",
				AllocatedStorage: 20,
				Tags:             map[string]string{},
				ReadReplica:      true,
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
				BackupRetentionPeriod: 21,
				Tags:                  map[string]string{},
				StorageType:           "gp3",
				AllocatedStorage:      20,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				LicenseModel:          "license-model",
				DbSubnetGroup:         "subnet-1",
				SecGroup:              "security-group-1",
				Salt:                  "salt",
				Password:              "encrypted-pw",
				ClearPassword:         "clear-pw",
				ReplicaDatabase:       "db-replica",
				AddReadReplica:        true,
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
				test.tags,
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
		currentPlan      catalog.RDSPlan
		newPlan          catalog.RDSPlan
		settings         *config.Settings
		expectedErr      error
		tags             map[string]string
	}{
		"sets plan properties": {
			options: Options{},
			existingInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						PlanID: "plan-1",
					},
				},
			},
			expectedInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						PlanID: "plan-2",
					},
				},
				SecGroup: "sec-group1",
				Tags:     map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan: catalog.RDSPlan{
				Plan: catalog.Plan{
					ID: "plan-2",
				},
				SecurityGroup: "sec-group1",
			},
			settings: &config.Settings{},
		},
		"update allocated storage": {
			options: Options{
				AllocatedStorage: 20,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
				Tags:             map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"allocated storage option less than existing, does not update": {
			options: Options{
				AllocatedStorage: 10,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectErr:   true,
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
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
				Tags:             map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"update backup retention period": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(20),
			},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 10,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
				Tags:                  map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"does not update backup retention period": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(0),
			},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
				Tags:                  map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"update binary log format": {
			options: Options{
				BinaryLogFormat: "ROW",
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				Tags:            map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"enable PG cron": {
			options: Options{
				EnablePgCron: aws.Bool(true),
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
				Tags:         map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"enable PG cron not specified": {
			options:          Options{},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				Tags: map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"enable PG cron not specified on options, true on existing instance": {
			options: Options{},
			existingInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
			},
			expectedInstance: &RDSInstance{
				Tags: map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"gp3 fails for allocated storage < 20": {
			options: Options{
				StorageType: "gp3",
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 10,
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
			expectErr:   true,
		},
		"gp3 upgrade succeeds": {
			options: Options{
				StorageType: "gp3",
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
				StorageType:      "gp2",
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
				StorageType:      "gp3",
				Tags:             map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings:    &config.Settings{},
		},
		"does not allow backup retention less than minimum backup retention": {
			options: Options{},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 0,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 14,
				Tags:                  map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings: &config.Settings{
				MinBackupRetention: 14,
			},
		},
		"update to plan with read replica enabled, instance has no replica": {
			options: Options{},
			existingInstance: &RDSInstance{
				Database: "db",
			},
			expectedInstance: &RDSInstance{
				Database:        "db",
				ReplicaDatabase: "db-replica",
				AddReadReplica:  true,
				Tags:            map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan: catalog.RDSPlan{
				ReadReplica: true,
				Redundant:   true,
			},
			settings: &config.Settings{},
		},
		"update to plan with read replica enabled, instance already has replica": {
			options: Options{},
			existingInstance: &RDSInstance{
				Database:        "db",
				ReplicaDatabase: "db-replica",
			},
			expectedInstance: &RDSInstance{
				Database:        "db",
				ReplicaDatabase: "db-replica",
				Tags:            map[string]string{},
			},
			currentPlan: catalog.RDSPlan{},
			newPlan: catalog.RDSPlan{
				ReadReplica: true,
				Redundant:   true,
			},
			settings: &config.Settings{},
		},
		"returns error if plan enables read replicas but is not multi-AZ": {
			options: Options{},
			existingInstance: &RDSInstance{
				Database: "db",
			},
			currentPlan: catalog.RDSPlan{},
			newPlan: catalog.RDSPlan{
				ReadReplica: true,
				Redundant:   false,
			},
			settings:  &config.Settings{},
			expectErr: true,
		},
		"update from plan with read replica enabled to non read-replica plan": {
			options: Options{},
			existingInstance: &RDSInstance{
				Database:        "db",
				ReplicaDatabase: "replica",
			},
			currentPlan: catalog.RDSPlan{
				ReadReplica: true,
			},
			newPlan:  catalog.RDSPlan{},
			settings: &config.Settings{},
			expectedInstance: &RDSInstance{
				Database:          "db",
				DeleteReadReplica: true,
				ReplicaDatabase:   "replica",
				Tags:              map[string]string{},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			modifiedInstance, err := test.existingInstance.modify(test.options, test.currentPlan, test.newPlan, test.settings, test.tags)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if diff := deep.Equal(modifiedInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestModifyInstanceRotateCredentials(t *testing.T) {
	testCases := map[string]struct {
		options                 Options
		currentPlan             catalog.RDSPlan
		newPlan                 catalog.RDSPlan
		settings                *config.Settings
		originalPassword        string
		originalSalt            string
		username                string
		shouldRotateCredentials bool
		tags                    map[string]string
	}{
		"rotate credentials": {
			options: Options{
				RotateCredentials: aws.Bool(true),
			},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
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
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			originalPassword:        helpers.RandStr(20),
			originalSalt:            helpers.RandStr(10),
			username:                helpers.RandStr(10),
			shouldRotateCredentials: false,
		},
		"rotate credentials not specified": {
			options:     Options{},
			currentPlan: catalog.RDSPlan{},
			newPlan:     catalog.RDSPlan{},
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
				dbUtils:       &RDSDatabaseUtils{},
			}
			modifiedInstance, err := existingInstance.modify(test.options, test.currentPlan, test.newPlan, test.settings, test.tags)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.shouldRotateCredentials && modifiedInstance.ClearPassword == test.originalPassword {
				t.Fatal("instance password should have been updated")
			}
			if test.shouldRotateCredentials && modifiedInstance.Salt == test.originalSalt {
				t.Fatal("instance salt should have been updated")
			}
		})
	}
}
