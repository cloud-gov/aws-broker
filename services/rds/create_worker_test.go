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
	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

func TestCreateWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		dbInstance    *RDSInstance
		expectedState base.InstanceState
		password      string
		plan          *catalog.RDSPlan
		expectErr     bool
		worker        *CreateWorker
	}{
		"success without replica": {
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
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceReady,
		},
		"success with replica": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: uuid.NewString(),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
				DbType:          "postgres",
				AddReadReplica:  true,
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceReady,
		},
		"error provisioning custom parameter group": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{
					returnErr: errors.New("failed"),
				},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
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
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"create DB error": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{
					createDbErr: errors.New("create database error"),
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
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
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"error waiting for database creation": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("fail")},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
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
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.worker.Work(test.ctx, &river.Job[CreateArgs]{Args: CreateArgs{
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

func TestPrepareCreateDbInstanceInput(t *testing.T) {
	testErr := errors.New("fail")
	testCases := map[string]struct {
		dbInstance        *RDSInstance
		worker            *CreateWorker
		expectedGroupName string
		expectedErr       error
		password          string
		expectedParams    *rds.CreateDBInstanceInput
		plan              *catalog.RDSPlan
		tags              map[string]string
	}{
		"expect error": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
				credentialUtils: &RDSCredentialUtils{},
			},
			worker: &CreateWorker{
				settings: &config.Settings{},
				rds:      &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					returnErr: testErr,
					rds:       &mockRDSClient{},
				},
			},
			plan:        &catalog.RDSPlan{},
			expectedErr: testErr,
		},
		"creates correct params": {
			dbInstance: &RDSInstance{
				AllocatedStorage:      10,
				Database:              "db-1",
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				credentialUtils:       &RDSCredentialUtils{},
				Username:              "fake-user",
				StorageType:           "storage-1",
				PubliclyAccessible:    true,
				BackupRetentionPeriod: 14,
				DbSubnetGroup:         "subnet-group-1",
				SecGroup:              "sec-group-1",
			},
			tags: map[string]string{
				"foo": "bar",
			},
			worker: &CreateWorker{
				settings: &config.Settings{
					PubliclyAccessibleFeature: true,
				},
				rds: &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds:              &mockRDSClient{},
					customPgroupName: "parameter-group-1",
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class-1",
				Redundant:     true,
				Encrypted:     true,
			},
			password: "fake-password",
			expectedParams: &rds.CreateDBInstanceInput{
				AllocatedStorage:        aws.Int32(10),
				DBInstanceClass:         aws.String("class-1"),
				DBInstanceIdentifier:    aws.String("db-1"),
				DBName:                  aws.String("db1"),
				Engine:                  aws.String("mysql"),
				MasterUserPassword:      aws.String("fake-password"),
				MasterUsername:          aws.String("fake-user"),
				AutoMinorVersionUpgrade: aws.Bool(true),
				MultiAZ:                 aws.Bool(true),
				StorageEncrypted:        aws.Bool(true),
				StorageType:             aws.String("storage-1"),
				Tags: []rdsTypes.Tag{
					{
						Key:   aws.String("foo"),
						Value: aws.String("bar"),
					},
				},
				PubliclyAccessible:    aws.Bool(true),
				BackupRetentionPeriod: aws.Int32(14),
				DBSubnetGroupName:     aws.String("subnet-group-1"),
				VpcSecurityGroupIds: []string{
					"sec-group-1",
				},
				DBParameterGroupName: aws.String("parameter-group-1"),
			},
		},
		"handles optional params": {
			dbInstance: &RDSInstance{
				AllocatedStorage:      10,
				Database:              "db-1",
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				DbVersion:             "8.0",
				credentialUtils:       &RDSCredentialUtils{},
				Username:              "fake-user",
				StorageType:           "storage-1",
				PubliclyAccessible:    true,
				BackupRetentionPeriod: 14,
				DbSubnetGroup:         "subnet-group-1",
				SecGroup:              "sec-group-1",
				LicenseModel:          "foo",
			},
			tags: map[string]string{
				"foo": "bar",
			},
			worker: &CreateWorker{
				settings: &config.Settings{
					PubliclyAccessibleFeature: true,
				},
				rds: &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{
					rds:              &mockRDSClient{},
					customPgroupName: "parameter-group-1",
				},
			},
			plan: &catalog.RDSPlan{
				InstanceClass: "class-1",
				Redundant:     true,
				Encrypted:     true,
			},
			password: "fake-password",
			expectedParams: &rds.CreateDBInstanceInput{
				AllocatedStorage:        aws.Int32(10),
				DBInstanceClass:         aws.String("class-1"),
				DBInstanceIdentifier:    aws.String("db-1"),
				DBName:                  aws.String("db1"),
				Engine:                  aws.String("mysql"),
				MasterUserPassword:      aws.String("fake-password"),
				MasterUsername:          aws.String("fake-user"),
				AutoMinorVersionUpgrade: aws.Bool(true),
				MultiAZ:                 aws.Bool(true),
				StorageEncrypted:        aws.Bool(true),
				StorageType:             aws.String("storage-1"),
				Tags: []rdsTypes.Tag{
					{
						Key:   aws.String("foo"),
						Value: aws.String("bar"),
					},
				},
				PubliclyAccessible:    aws.Bool(true),
				BackupRetentionPeriod: aws.Int32(14),
				DBSubnetGroupName:     aws.String("subnet-group-1"),
				VpcSecurityGroupIds: []string{
					"sec-group-1",
				},
				DBParameterGroupName: aws.String("parameter-group-1"),
				EngineVersion:        aws.String("8.0"),
				LicenseModel:         aws.String("foo"),
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			test.dbInstance.setTags(test.plan, test.tags)
			params, err := test.worker.prepareCreateDbInput(test.dbInstance, test.plan, test.password)
			if err != nil && test.expectedErr == nil {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if test.expectedErr != nil && (err == nil || err.Error() != test.expectedErr.Error()) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if diff := deep.Equal(params, test.expectedParams); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestAsyncCreateDb(t *testing.T) {
	createDbErr := errors.New("create DB error")

	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		worker        *CreateWorker
		dbInstance    *RDSInstance
		expectedState base.InstanceState
		password      string
		plan          *catalog.RDSPlan
		expectErr     bool
	}{
		"error provisioning custom parameter group": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{
					returnErr: errors.New("failed"),
				},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			}),
			plan:          &catalog.RDSPlan{},
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"create DB error": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{
					createDbErr: createDbErr,
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			}),
			plan:          &catalog.RDSPlan{},
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"error waiting for database creation": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("fail")},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			}),
			plan:          &catalog.RDSPlan{},
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"success without replica": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			password: helpers.RandStr(10),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			}),
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceReady,
		},
		"success with replica": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			plan:     &catalog.RDSPlan{},
			password: helpers.RandStr(10),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "replica",
				AddReadReplica:  true,
				credentialUtils: &RDSCredentialUtils{},
			}),
			expectedState: base.InstanceReady,
		},
		"error creating replica": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
					createDBInstanceReadReplicaErrs: []error{errors.New("fail")},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{
					mockClearPassword: "fake-pw",
				},
			),
			plan:     &catalog.RDSPlan{},
			password: helpers.RandStr(10),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "replica",
				AddReadReplica:  true,
				credentialUtils: &RDSCredentialUtils{},
			}),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"error getting password": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{
					mockGetPassworrdErr: errors.New("error getting password"),
				},
			),
			dbInstance: createTestRdsInstance(&RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				credentialUtils: &RDSCredentialUtils{},
			}),
			plan:          &catalog.RDSPlan{},
			password:      helpers.RandStr(10),
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.worker.asyncCreateDB(test.ctx, test.dbInstance, test.plan)
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}

			if test.expectErr && err == nil {
				t.Fatal("expected error")
			}

			asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
			if err != nil {
				t.Fatal(err)
			}

			if test.expectedState != asyncJobMsg.JobState.State {
				t.Fatalf("expected async job state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}
		})
	}
}

func TestCreateDBReadReplica(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx        context.Context
		worker     *CreateWorker
		dbInstance *RDSInstance
		expectErr  bool
		plan       *catalog.RDSPlan
	}{
		"success": {
			ctx: t.Context(),
			worker: &CreateWorker{
				db: brokerDB,
				settings: &config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				rds:                  &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{},
				logger:               slog.New(&testutil.MockLogHandler{}),
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			plan: &catalog.RDSPlan{},
		},
		"success on retry": {
			ctx: t.Context(),
			worker: &CreateWorker{
				db: brokerDB,
				settings: &config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				rds:                  &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{},
				logger:               slog.New(&testutil.MockLogHandler{}),
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			plan: &catalog.RDSPlan{},
		},
		"gives up after maximum retries": {
			ctx: t.Context(),
			worker: &CreateWorker{
				db: brokerDB,
				settings: &config.Settings{
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 10 * time.Millisecond,
				},
				rds: &mockRDSClient{
					createDBInstanceReadReplicaErrs: []error{
						&rdsTypes.InvalidDBInstanceStateFault{},
						&rdsTypes.InvalidDBInstanceStateFault{},
						&rdsTypes.InvalidDBInstanceStateFault{},
						&rdsTypes.InvalidDBInstanceStateFault{},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				logger:               slog.New(&testutil.MockLogHandler{}),
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			plan:      &catalog.RDSPlan{},
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := test.worker.createDBReadReplica(test.ctx, test.dbInstance, test.plan)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}
			if test.expectErr && err == nil {
				t.Fatal("expected error but received nil")
			}
		})
	}
}

func TestWaitAndCreateDBReadReplica(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		worker        *CreateWorker
		dbInstance    *RDSInstance
		expectedState base.InstanceState
		expectErr     bool
		plan          *catalog.RDSPlan
	}{
		"success": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceInProgress,
		},
		"error checking database creation status": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
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
					},
					describeDbInstancesErrs: []error{nil, errors.New("error describing database instances")},
				},
				slog.New(&testutil.MockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"error creating database replica": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{
					createDBInstanceReadReplicaErrs: []error{errors.New("error creating database instance read replica")},
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
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
				&mockCredentialUtils{},
			),
			plan: &catalog.RDSPlan{},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
		"error adding tags": {
			ctx: t.Context(),
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 3 * time.Millisecond,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
				},
				&mockRDSClient{
					addTagsToResourceErr: errors.New("error adding tags to read replica"),
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
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceNotCreated,
			expectErr:     true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.worker.waitAndCreateDBReadReplica(test.ctx, base.CreateOp, test.dbInstance, test.plan)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
			if err != nil {
				t.Fatal(err)
			}

			if asyncJobMsg.JobState.State != test.expectedState {
				t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}
		})
	}
}
