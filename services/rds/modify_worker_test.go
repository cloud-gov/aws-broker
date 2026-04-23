package rds

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/go-test/deep"
)

func TestAsyncModifyDb(t *testing.T) {
	modifyDbErr := errors.New("modify DB error")
	dbInstanceNotFoundErr := &rdsTypes.DBInstanceNotFoundFault{
		Message: aws.String("operation failed"),
	}

	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	testCases := map[string]struct {
		dbInstance         *RDSInstance
		expectedState      base.InstanceState
		expectedDbInstance *RDSInstance
		plan               *catalog.RDSPlan
		worker             *ModifyWorker
		ctx                context.Context
		expectErr          bool
	}{
		"error preparing modify input": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				logger,
				&mockParameterGroupClient{
					returnErr: errors.New("fail"),
				},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotModified,
			ctx:           context.Background(),
			expectErr:     true,
		},
		"modify primary DB error": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					modifyDbErrs: []error{modifyDbErr},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotModified,
			ctx:           context.Background(),
			expectErr:     true,
		},
		"error waiting for database to be ready": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("fail")},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotModified,
			ctx:           context.Background(),
			expectErr:     true,
		},
		"success without read replica": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			plan: &catalog.RDSPlan{},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
			},
			expectedState: base.InstanceReady,
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
			},
			ctx: context.Background(),
		},
		"success with adding read replica": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-2",
					},
					Uuid: "uuid-2",
				},
				Database:        "db-2",
				AddReadReplica:  true,
				ReplicaDatabase: "db-replica",
				credentialUtils: &RDSCredentialUtils{},
			},
			expectedState: base.InstanceReady,
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-2",
					},
					Uuid: "uuid-2",
				},
				Database:        "db-2",
				ReplicaDatabase: "db-replica",
				credentialUtils: &RDSCredentialUtils{},
			},
			plan: &catalog.RDSPlan{},
			ctx:  context.Background(),
		},
		"error modifying read replica": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
					modifyDbErrs: []error{nil, modifyDbErr},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "db-replica",
				credentialUtils: &RDSCredentialUtils{},
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotModified,
			ctx:           context.Background(),
			expectErr:     true,
		},
		"error creating read replica": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
					createDBInstanceReadReplicaErrs: []error{errors.New("error creating read replica")},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				AddReadReplica:  true,
				ReplicaDatabase: "db-replica",
				credentialUtils: &RDSCredentialUtils{},
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotModified,
			ctx:           context.Background(),
			expectErr:     true,
		},
		"success with deleting read replica": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
					describeDbInstancesErrs: []error{nil, nil, dbInstanceNotFoundErr},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-3",
					},
					Uuid: "uuid-3",
				},
				Database:          "db-3",
				DeleteReadReplica: true,
				ReplicaDatabase:   "db-replica",
				credentialUtils:   &RDSCredentialUtils{},
			},
			expectedState: base.InstanceReady,
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-3",
					},
					Uuid: "uuid-3",
				},
				Database:        "db-3",
				credentialUtils: &RDSCredentialUtils{},
			},
			plan: &catalog.RDSPlan{},
			ctx:  context.Background(),
		},
		"error updating read replica tags": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
					addTagsToResourceErr: errors.New("error updating tags"),
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "db-replica",
				credentialUtils: &RDSCredentialUtils{},
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotModified,
			ctx:           context.Background(),
			expectErr:     true,
		},
		"success without read replica and updating version": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				logger,
				&mockParameterGroupClient{},
				&RDSCredentialUtils{},
			),
			plan: &catalog.RDSPlan{},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
				DbVersion:       "9.0",
			},
			expectedState: base.InstanceReady,
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
				DbVersion:       "9.0",
			},
			ctx: context.Background(),
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.worker.asyncModifyDb(test.ctx, test.dbInstance, test.plan)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error but received none")
			}

			asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.ModifyOp)
			if err != nil {
				t.Fatal(err)
			}

			if test.expectedState != asyncJobMsg.JobState.State {
				t.Fatalf("expected async job state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}

			if test.expectedDbInstance != nil {
				updatedInstance := RDSInstance{}
				err = brokerDB.Where("uuid = ?", test.dbInstance.Uuid).First(&updatedInstance).Error
				if err != nil {
					t.Fatal(err)
				}

				if diff := deep.Equal(&updatedInstance, test.expectedDbInstance); diff != nil {
					t.Error(diff)
				}
			}
		})
	}
}

func TestPrepareModifyDbInstanceInput(t *testing.T) {
	testErr := errors.New("fail")
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		dbInstance        *RDSInstance
		worker            *ModifyWorker
		expectedGroupName string
		expectedErr       error
		expectedParams    *rds.ModifyDBInstanceInput
		plan              *catalog.RDSPlan
		isReplica         bool
	}{
		"expect returned group name": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				credentialUtils:       &RDSCredentialUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				DbVersion:             "8.0",
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					customPgroupName: "foobar",
					rds:              &mockRDSClient{},
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			expectedGroupName: "foobar",
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int32(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int32(14),
				DBParameterGroupName:     aws.String("foobar"),
				EngineVersion:            aws.String("8.0"),
			},
		},
		"expect error": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				credentialUtils:       &RDSCredentialUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds:       &mockRDSClient{},
					returnErr: testErr,
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			expectedErr: testErr,
		},
		"update password": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
				credentialUtils: &mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				RotateCredentials:     true,
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int32(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int32(14),
				MasterUserPassword:       aws.String("fake-pw"),
			},
		},
		"update storage type": {
			dbInstance: &RDSInstance{
				credentialUtils:       &RDSCredentialUtils{},
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int32(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int32(14),
				StorageType:              aws.String("gp3"),
			},
		},
		"update engine version": {
			dbInstance: &RDSInstance{
				credentialUtils:       &RDSCredentialUtils{},
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				DbVersion:             "9.0",
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int32(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int32(14),
				StorageType:              aws.String("gp3"),
				EngineVersion:            aws.String("9.0"),
			},
		},
		"does not update password for replica": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				credentialUtils:       &RDSCredentialUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			isReplica: true,
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int32(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int32(14),
			},
		},
		"allow major version upgrade": {
			dbInstance: &RDSInstance{
				credentialUtils:          &RDSCredentialUtils{},
				DbType:                   "mysql",
				StorageType:              "gp3",
				AllocatedStorage:         20,
				Database:                 "db-name",
				BackupRetentionPeriod:    14,
				DbVersion:                "9.0",
				AllowMajorVersionUpgrade: true,
			},
			worker: &ModifyWorker{
				db:       brokerDB,
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class",
				Redundant:     true,
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int32(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(true),
				BackupRetentionPeriod:    aws.Int32(14),
				StorageType:              aws.String("gp3"),
				EngineVersion:            aws.String("9.0"),
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := test.worker.prepareModifyDbInstanceInput(test.dbInstance, test.plan, test.dbInstance.Database, test.isReplica)
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedParams != nil {
				if diff := deep.Equal(params, test.expectedParams); diff != nil {
					t.Error(diff)
				}
			}
		})
	}
}
