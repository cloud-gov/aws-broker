package rds

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/go-test/deep"
	"github.com/lib/pq"
)

func TestInit(t *testing.T) {
	testCases := map[string]struct {
		options          Options
		rdsInstance      *RDSInstance
		expectedInstance *RDSInstance
		expectErr        bool
		plan             *catalog.RDSPlan
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
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				Adapter:          "adapter-1",
				DbType:           "postgres",
				DbVersion:        "16",
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
				credentialUtils: &mockCredentialUtils{
					mockSalt:              "salt",
					mockEncryptedPassword: "encrypted-pw",
				},
			},
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
				Adapter:               "adapter-1",
				DbType:                "postgres",
				DbVersion:             "16",
				BackupRetentionPeriod: 21,
				StorageType:           "gp3",
				AllocatedStorage:      20,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				LicenseModel:          "license-model",
				DbSubnetGroup:         "subnet-1",
				SecGroup:              "security-group-1",
				Salt:                  "salt",
				Password:              "encrypted-pw",
				Tags:                  map[string]string{},
			},
		},
		"MySQL sets db version from plan": {
			options: Options{},
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				DbType:                "mysql",
				DbVersion:             "8.0",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 14,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				credentialUtils: &mockCredentialUtils{},
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
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				DbType:                "mysql",
				DbVersion:             "8.0",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 14,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				credentialUtils: &mockCredentialUtils{},
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
				Version: "16",
			},
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				DbType:                "postgres",
				DbVersion:             "16",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 14,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				credentialUtils: &mockCredentialUtils{},
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
				DbVersion:             "16",
				BackupRetentionPeriod: 14,
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
		},
		"sets backup retention period from plan": {
			options: Options{},
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				DbType:                "postgres",
				DbVersion:             "16",
				AllocatedStorage:      20,
				BackupRetentionPeriod: 23,
			},
			settings: &config.Settings{},
			rdsInstance: &RDSInstance{
				credentialUtils: &mockCredentialUtils{},
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
				DbVersion:             "16",
				BackupRetentionPeriod: 23,
				AllocatedStorage:      20,
				Tags:                  map[string]string{},
			},
		},
		"merges plan and instance tags": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(14),
			},
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				Adapter:          "adapter-1",
				DbType:           "postgres",
				DbVersion:        "16",
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
				credentialUtils: &mockCredentialUtils{
					mockSalt:              "salt",
					mockEncryptedPassword: "encrypted-pw",
				},
			},
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
				Adapter:               "adapter-1",
				DbType:                "postgres",
				DbVersion:             "16",
				BackupRetentionPeriod: 14,
				StorageType:           "gp3",
				AllocatedStorage:      20,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				LicenseModel:          "license-model",
				DbSubnetGroup:         "subnet-1",
				SecGroup:              "security-group-1",
				Salt:                  "salt",
				Password:              "encrypted-pw",
				Tags: map[string]string{
					"plan-tag": "random-value",
					"foo":      "bar",
				},
			},
		},
		"plan has read replica enabled": {
			options: Options{
				BackupRetentionPeriod: aws.Int64(21),
			},
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-1",
				},
				Adapter:          "adapter-1",
				DbType:           "postgres",
				DbVersion:        "16",
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
				credentialUtils: &mockCredentialUtils{
					mockSalt:              "salt",
					mockEncryptedPassword: "encrypted-pw",
				},
			},
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
				Adapter:               "adapter-1",
				DbType:                "postgres",
				DbVersion:             "16",
				BackupRetentionPeriod: 21,
				StorageType:           "gp3",
				AllocatedStorage:      20,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				LicenseModel:          "license-model",
				DbSubnetGroup:         "subnet-1",
				SecGroup:              "security-group-1",
				Salt:                  "salt",
				Password:              "encrypted-pw",
				AddReadReplica:        true,
				Tags:                  map[string]string{},
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
		currentPlan      *catalog.RDSPlan
		newPlan          *catalog.RDSPlan
		settings         *config.Settings
		expectedErr      error
		tags             map[string]string
		expectUpdates    bool
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
			currentPlan: &catalog.RDSPlan{},
			newPlan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: "plan-2",
				},
				SecurityGroup: "sec-group1",
			},
			settings:      &config.Settings{},
			expectUpdates: true,
		},
		"update allocated storage": {
			options: Options{
				AllocatedStorage: 30,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 30,
				Tags:             map[string]string{},
			},
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
		},
		"allocated storage option less than existing, does not update": {
			options: Options{
				AllocatedStorage: 10,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
				Tags:             map[string]string{},
			},
			expectErr:   true,
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
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
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
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
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
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
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
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
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
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
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
		},
		"enable PG cron not specified": {
			options:          Options{},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				Tags: map[string]string{},
			},
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
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
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
		},
		"gp3 fails for allocated storage < 20": {
			options: Options{
				StorageType: "gp3",
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 10,
			},
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
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
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
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
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
			settings: &config.Settings{
				MinBackupRetention: 14,
			},
			expectUpdates: true,
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
			currentPlan: &catalog.RDSPlan{},
			newPlan: &catalog.RDSPlan{
				ReadReplica: true,
				Redundant:   true,
			},
			settings:      &config.Settings{},
			expectUpdates: true,
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
			currentPlan: &catalog.RDSPlan{},
			newPlan: &catalog.RDSPlan{
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
			currentPlan: &catalog.RDSPlan{},
			newPlan: &catalog.RDSPlan{
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
			currentPlan: &catalog.RDSPlan{
				ReadReplica: true,
			},
			newPlan:  &catalog.RDSPlan{},
			settings: &config.Settings{},
			expectedInstance: &RDSInstance{
				Database:          "db",
				DeleteReadReplica: true,
				ReplicaDatabase:   "replica",
				Tags:              map[string]string{},
			},
			expectUpdates: true,
		},
		"updates tags": {
			options: Options{},
			existingInstance: &RDSInstance{
				Database:        "db",
				ReplicaDatabase: "replica",
			},
			currentPlan: &catalog.RDSPlan{
				ReadReplica: true,
			},
			newPlan: &catalog.RDSPlan{
				Tags: map[string]string{
					"foo": "bar",
				},
			},
			tags: map[string]string{
				"foo2": "baz",
			},
			settings: &config.Settings{},
			expectedInstance: &RDSInstance{
				Database:          "db",
				DeleteReadReplica: true,
				ReplicaDatabase:   "replica",
				Tags: map[string]string{
					"foo":  "bar",
					"foo2": "baz",
				},
			},
			expectUpdates: true,
		},
		"update MySQL database version from options": {
			options: Options{
				Version: "9.0",
			},
			existingInstance: &RDSInstance{
				DbType:    "mysql",
				DbVersion: "8.0",
			},
			expectedInstance: &RDSInstance{
				DbType:    "mysql",
				DbVersion: "9.0",
				Tags:      map[string]string{},
			},
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
		},
		"update PostgreSQL database version from options": {
			options: Options{
				Version: "9.0",
			},
			existingInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "8.0",
			},
			expectedInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "9.0",
				Tags:      map[string]string{},
			},
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
		},
		"update PostgreSQL database version from plan": {
			options: Options{},
			existingInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "8.0",
			},
			expectedInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "8.0",
				Tags:      map[string]string{},
			},
			currentPlan: &catalog.RDSPlan{},
			newPlan: &catalog.RDSPlan{
				DbVersion: "9.0",
			},
			settings:      &config.Settings{},
			expectUpdates: false,
		},
		"update MySQL database version from plan": {
			options: Options{},
			existingInstance: &RDSInstance{
				DbType:    "mysql",
				DbVersion: "8.0",
			},
			expectedInstance: &RDSInstance{
				DbType:    "mysql",
				DbVersion: "8.0",
				Tags:      map[string]string{},
			},
			currentPlan: &catalog.RDSPlan{},
			newPlan: &catalog.RDSPlan{
				DbVersion: "9.0",
			},
			settings:      &config.Settings{},
			expectUpdates: false,
		},
		"update allows major version upgrade": {
			options: Options{
				Version:                  "9.0",
				AllowMajorVersionUpgrade: aws.Bool(true),
			},
			existingInstance: &RDSInstance{
				DbType:    "postgres",
				DbVersion: "8.0",
			},
			expectedInstance: &RDSInstance{
				DbType:                   "postgres",
				DbVersion:                "9.0",
				AllowMajorVersionUpgrade: true,
				Tags:                     map[string]string{},
			},
			currentPlan:   &catalog.RDSPlan{},
			newPlan:       &catalog.RDSPlan{},
			settings:      &config.Settings{},
			expectUpdates: true,
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

			if test.expectUpdates {
				if diff := deep.Equal(test.existingInstance, test.expectedInstance); diff == nil {
					t.Error("Expected no modifications to existing instance")
					t.Error(diff)
				}
			}

			if diff := deep.Equal(modifiedInstance, test.expectedInstance); diff != nil {
				t.Fatal(diff)
			}
		})
	}
}

func TestModifyInstanceRotateCredentials(t *testing.T) {
	testCases := map[string]struct {
		options                 Options
		currentPlan             *catalog.RDSPlan
		newPlan                 *catalog.RDSPlan
		settings                *config.Settings
		shouldRotateCredentials bool
		tags                    map[string]string
		existingInstance        *RDSInstance
	}{
		"rotate credentials": {
			options: Options{
				RotateCredentials: aws.Bool(true),
			},
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			shouldRotateCredentials: true,
			existingInstance: &RDSInstance{
				Username:        helpers.RandStr(10),
				Salt:            helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
				Password:        helpers.RandStr(10),
			},
		},
		"do not rotate credentials": {
			options: Options{
				RotateCredentials: aws.Bool(false),
			},
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			shouldRotateCredentials: false,
			existingInstance: &RDSInstance{
				Salt:     helpers.RandStr(10),
				Username: helpers.RandStr(10),
				Password: helpers.RandStr(10),
			},
		},
		"rotate credentials not specified": {
			options:     Options{},
			currentPlan: &catalog.RDSPlan{},
			newPlan:     &catalog.RDSPlan{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
			},
			existingInstance: &RDSInstance{
				Salt:     helpers.RandStr(10),
				Username: helpers.RandStr(10),
				Password: helpers.RandStr(10),
			},
			shouldRotateCredentials: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			modifiedInstance, err := test.existingInstance.modify(test.options, test.currentPlan, test.newPlan, test.settings, test.tags)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.shouldRotateCredentials != modifiedInstance.RotateCredentials {
				t.Fatalf("mismatch of instance RotateCredentials value, expected: %t, got: %t", test.shouldRotateCredentials, modifiedInstance.RotateCredentials)
			}
			if test.shouldRotateCredentials && modifiedInstance.Password == test.existingInstance.Password {
				t.Fatal("instance password should have been updated")
			}
			if test.shouldRotateCredentials && modifiedInstance.Salt == test.existingInstance.Salt {
				t.Fatal("instance salt should have been updated")
			}
		})
	}
}

func TestSetTagsConcurrency(t *testing.T) {
	var wg sync.WaitGroup

	plan := &catalog.RDSPlan{
		Tags: map[string]string{
			"foo": "bar",
		},
	}

	updateInstanceTags := func(tags map[string]string, expectedTags map[string]string, wg *sync.WaitGroup) {
		defer wg.Done()
		i := &RDSInstance{}
		i.setTags(plan, tags)

		updatedTags := i.getTags()

		if diff := deep.Equal(expectedTags, updatedTags); diff != nil {
			t.Error(diff)
		}
	}

	// Launch two goroutines
	wg.Add(2)

	go updateInstanceTags(map[string]string{"moo": "cow"}, map[string]string{"foo": "bar", "moo": "cow"}, &wg)
	go updateInstanceTags(map[string]string{"foo2": "bar2"}, map[string]string{"foo": "bar", "foo2": "bar2"}, &wg)

	wg.Wait()
}

func TestRDSInstanceMarshalAndUnmarshal(t *testing.T) {
	i := &RDSInstance{
		Instance: base.Instance{
			Uuid: "uuid-1",
			Request: request.Request{
				ServiceID: "service-1",
			},
		},
		AllocatedStorage:                 20,
		Database:                         "db",
		DbType:                           "type1",
		Username:                         "user1",
		Password:                         "fake-pw",
		Salt:                             "fake-salt",
		EnabledCloudwatchLogGroupExports: pq.StringArray{"postgres"},
		BackupRetentionPeriod:            14,
		StorageType:                      "gp3",
		DbSubnetGroup:                    "group-1",
		SecGroup:                         "sec-group-1",
		LicenseModel:                     "license",
		BinaryLogFormat:                  "format",
		EnablePgCron:                     aws.Bool(false),
		ParameterGroupFamily:             "postgres16",
		ParameterGroupName:               "parameter-group-1",
		ReplicaDatabase:                  "replica",
		ReplicaDatabaseHost:              "host",
	}
	i.setTags(&catalog.RDSPlan{}, map[string]string{
		"foo": "bar",
	})
	output, err := json.Marshal(i)
	if err != nil {
		t.Fatal(err)
	}
	expectedProperties := []string{
		`"Database": "db"`,
		`"DbType": "type1"`,
		`"Tags": {"foo": "bar"}`,
		`"EnabledCloudwatchLogGroupExports":["postgres"]`,
		`"Uuid": "uuid-1"`,
		`"service_id":"service-1"`,
		`"AllocatedStorage":20`,
		`"BackupRetentionPeriod":14`,
		`"StorageType":"gp3"`,
		`"PubliclyAccessible":false`,
		`"Password":"fake-pw"`,
		`"Salt":"fake-salt"`,
		`"DbSubnetGroup":"group-1"`,
		`"SecGroup":"sec-group-1"`,
		`"EnableFunctions":false`,
		`"LicenseModel":"license"`,
		`"BinaryLogFormat":"format"`,
		`"EnablePgCron":false`,
		`"ParameterGroupFamily": "postgres16"`,
		`"ParameterGroupName": "parameter-group-1"`,
		`"AddReadReplica":false`,
		`"ReplicaDatabase": "replica"`,
		`"ReplicaDatabaseHost": "host"`,
		`"DeleteReadReplica":false`,
		`"RotateCredentials":false`,
		`"AllowMajorVersionUpgrade":false`,
	}
	for _, property := range expectedProperties {
		if !strings.Contains(string(output), strings.ReplaceAll(property, " ", "")) {
			t.Fatalf("could not find %s in marshaled JSON", property)
		}
	}
	unmarshaledInstance := &RDSInstance{}
	json.Unmarshal(output, unmarshaledInstance)
	if diff := deep.Equal(i, unmarshaledInstance); diff != nil {
		t.Fatal(diff)
	}
}
