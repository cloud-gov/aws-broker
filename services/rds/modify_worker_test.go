package rds

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
	"github.com/lib/pq"
	"github.com/riverqueue/river"
)

func TestModifyWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		dbInstance    *RDSInstance
		expectedState base.InstanceState
		password      string
		expectErr     bool
		worker        *ModifyWorker
		plan          *catalog.RDSPlan
	}{
		"success": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
				DbType:          "postgres",
			},
			plan: &catalog.RDSPlan{},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
			expectedState: base.InstanceReady,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.worker.Work(test.ctx, &river.Job[ModifyArgs]{Args: ModifyArgs{
				Instance: test.dbInstance,
				Plan:     test.plan,
			}})
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAsyncModifyDb(t *testing.T) {
	modifyDbErr := errors.New("modify DB error")
	dbInstanceNotFoundErr := &rdsTypes.DBInstanceNotFoundFault{
		Message: aws.String("operation failed"),
	}

	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{
					provisionOrModifyParamGroupErr: errors.New("fail"),
				},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
			expectErr:     true,
		},
		"modify primary DB error": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					modifyDbErrs: []error{modifyDbErr},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
			expectErr:     true,
		},
		"error waiting for database to be ready": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("fail")},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx: t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:  t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:  t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockOptionGroupClient{},
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
			ctx: t.Context(),
		},
		"applies updated parameter group": {
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMaxRetries:  1,
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
									DBParameterGroups: []rdsTypes.DBParameterGroupStatus{
										{
											ParameterApplyStatus: aws.String("in-sync"),
										},
									},
								},
							},
						},
					},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{
					customPgroupName: "new-group",
				},
				&mockOptionGroupClient{},
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
			ctx:           t.Context(),
		},
		"error deleting old parameter group": {
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
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{
					deleteParameterGroupErr: errors.New("failed to delete"),
				},
				&mockOptionGroupClient{},
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
				Database:           "db-1",
				credentialUtils:    &RDSCredentialUtils{},
				DbVersion:          "9.0",
				ParameterGroupName: "existing-group",
			},
			expectedState: base.InstanceNotModified,
			expectErr:     true,
			ctx:           t.Context(),
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

			asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.ModifyOp)
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
		"update password": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				RotateCredentials:     true,
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
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
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
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
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				DbVersion:             "9.0",
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
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
		"sets option gruop for an instance with a custom option group": {
			dbInstance: &RDSInstance{
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				DbVersion:             "8.4.9",
				OptionGroupName:       "my-audit-group",
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{
					optionGroupName: "cg-aws-broker-db-name-option-8-4",
				},
				&mockCredentialUtils{},
			),
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
				EngineVersion:            aws.String("8.4.9"),
				OptionGroupName:          aws.String("cg-aws-broker-db-name-option-8-4"),
			},
		},
		"does not update password for replica": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
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
		"enables cloudwatch log exports": {
			dbInstance: &RDSInstance{
				DbType:                           "postgres",
				AllocatedStorage:                 20,
				Database:                         "db-name",
				BackupRetentionPeriod:            14,
				EnabledCloudwatchLogGroupExports: pq.StringArray{"postgresql", "upgrade"},
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
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
				CloudwatchLogsExportConfiguration: &rdsTypes.CloudwatchLogsExportConfiguration{
					EnableLogTypes: []string{"postgresql", "upgrade"},
				},
			},
		},
		"allow major version upgrade": {
			dbInstance: &RDSInstance{
				DbType:                   "mysql",
				StorageType:              "gp3",
				AllocatedStorage:         20,
				Database:                 "db-name",
				BackupRetentionPeriod:    14,
				DbVersion:                "9.0",
				AllowMajorVersionUpgrade: true,
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
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
		"include parameter group": {
			dbInstance: &RDSInstance{
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
				DbVersion:             "9.0",
				ParameterGroupName:    "group1",
			},
			worker: NewModifyWorker(
				brokerDB,
				&config.Settings{},
				&mockRDSClient{},
				nil,
				&mockParameterGroupClient{
					customPgroupName: "group1",
					rds:              &mockRDSClient{},
				},
				&mockOptionGroupClient{},
				&mockCredentialUtils{},
			),
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
				DBParameterGroupName:     aws.String("group1"),
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
