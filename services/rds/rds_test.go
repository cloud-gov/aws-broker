package rds

import (
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/taskqueue"
	"github.com/go-test/deep"
)

func TestPrepareCreateDbInstanceInput(t *testing.T) {
	testErr := errors.New("fail")
	testCases := map[string]struct {
		dbInstance        *RDSInstance
		dbAdapter         *dedicatedDBAdapter
		expectedGroupName string
		expectedErr       error
		password          string
		expectedParams    *rds.CreateDBInstanceInput
	}{
		"expect error": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					returnErr: testErr,
					rds:       &mockRDSClient{},
				},
			},
			expectedErr: testErr,
		},
		"creates correct params": {
			dbInstance: &RDSInstance{
				AllocatedStorage: 10,
				Database:         "db-1",
				BinaryLogFormat:  "ROW",
				DbType:           "mysql",
				dbUtils: &MockDbUtils{
					mockFormattedDbName: "formatted-name",
				},
				Username:    "fake-user",
				StorageType: "storage-1",
				Tags: map[string]string{
					"foo": "bar",
				},
				PubliclyAccessible:    true,
				BackupRetentionPeriod: 14,
				DbSubnetGroup:         "subnet-group-1",
				SecGroup:              "sec-group-1",
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					rds:              &mockRDSClient{},
					customPgroupName: "parameter-group-1",
				},
				Plan: catalog.RDSPlan{
					InstanceClass: "class-1",
					Redundant:     true,
					Encrypted:     true,
				},
				settings: config.Settings{
					PubliclyAccessibleFeature: true,
				},
			},
			password: "fake-password",
			expectedParams: &rds.CreateDBInstanceInput{
				AllocatedStorage:        aws.Int64(10),
				DBInstanceClass:         aws.String("class-1"),
				DBInstanceIdentifier:    aws.String("db-1"),
				DBName:                  aws.String("formatted-name"),
				Engine:                  aws.String("mysql"),
				MasterUserPassword:      aws.String("fake-password"),
				MasterUsername:          aws.String("fake-user"),
				AutoMinorVersionUpgrade: aws.Bool(true),
				MultiAZ:                 aws.Bool(true),
				StorageEncrypted:        aws.Bool(true),
				StorageType:             aws.String("storage-1"),
				Tags: []*rds.Tag{
					{
						Key:   aws.String("foo"),
						Value: aws.String("bar"),
					},
				},
				PubliclyAccessible:    aws.Bool(true),
				BackupRetentionPeriod: aws.Int64(14),
				DBSubnetGroupName:     aws.String("subnet-group-1"),
				VpcSecurityGroupIds: []*string{
					aws.String("sec-group-1"),
				},
				DBParameterGroupName: aws.String("parameter-group-1"),
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := test.dbAdapter.prepareCreateDbInput(test.dbInstance, test.password)
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

func TestCreateDb(t *testing.T) {
	createDbErr := errors.New("create DB error")
	testCases := map[string]struct {
		dbInstance             *RDSInstance
		dbAdapter              *dedicatedDBAdapter
		expectedErr            error
		expectedState          base.InstanceState
		password               string
		expectedAsyncJobStates []base.InstanceState
	}{
		"create DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					createDbErr: createDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
				dbUtils:  &RDSDatabaseUtils{},
			},
			password:      helpers.RandStr(10),
			expectedErr:   createDbErr,
			expectedState: base.InstanceNotCreated,
		},
		"success without replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        0,
				},
			},
			password: helpers.RandStr(10),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
				dbUtils:  &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceInProgress,
		},
		"success with replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
			},
			password: helpers.RandStr(10),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "replica",
				AddReadReplica:  true,
				dbUtils:         &RDSDatabaseUtils{},
			},
			expectedState:          base.InstanceInProgress,
			expectedAsyncJobStates: []base.InstanceState{base.InstanceInProgress, base.InstanceReady},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			responseCode, err := test.dbAdapter.createDB(test.dbInstance, test.password, brokerDB)

			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			if len(test.expectedAsyncJobStates) > 0 {
				asyncJobMsg, err := taskqueue.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
				if err != nil {
					t.Fatal(err)
				}

				// The exact database state at this point in the test is non-deterministic, since the database updates
				// are being done in a goroutine. So we test against a set of possible job states
				if !slices.Contains(test.expectedAsyncJobStates, asyncJobMsg.JobState.State) {
					t.Fatalf("expected one of async job states: %+v, got: %s", test.expectedAsyncJobStates, asyncJobMsg.JobState.State)
				}
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
		})
	}
}

func TestWaitForDbReady(t *testing.T) {
	testCases := map[string]struct {
		dbInstance            *RDSInstance
		dbAdapter             *dedicatedDBAdapter
		expectedState         base.InstanceState
		expectErr             bool
		expectAsyncJobMessage bool
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
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
		},
		"waits with retries for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("creating"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("creating"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        3,
				},
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
		},
		"gives up after maximum retries for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("creating"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("creating"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("creating"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        3,
				},
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
			expectedState:         base.InstanceNotCreated,
			expectErr:             true,
			expectAsyncJobMessage: true,
		},
		"error checking database creation status": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
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
			expectedState:         base.InstanceNotCreated,
			expectErr:             true,
			expectAsyncJobMessage: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			err = test.dbAdapter.waitForDbReady(brokerDB, base.CreateOp, test.dbInstance, test.dbInstance.Database)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			if test.expectAsyncJobMessage {
				asyncJobMsg, err := taskqueue.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
				if err != nil {
					t.Fatal(err)
				}

				if asyncJobMsg.JobState.State != test.expectedState {
					t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
				}
			}
		})
	}
}

func TestWaitAndCreateDBReadReplica(t *testing.T) {
	testCases := map[string]struct {
		dbInstance    *RDSInstance
		dbAdapter     *dedicatedDBAdapter
		expectedState base.InstanceState
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
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
			expectedState: base.InstanceReady,
		},
		"error checking database creation status": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
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
			expectedState: base.InstanceNotCreated,
		},
		"error creating database replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					createDBInstanceReadReplicaErr: errors.New("error creating database instance read replica"),
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
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
			expectedState: base.InstanceNotCreated,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			test.dbAdapter.waitAndCreateDBReadReplica(brokerDB, base.CreateOp, test.dbInstance)

			asyncJobMsg, err := taskqueue.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
			if err != nil {
				t.Fatal(err)
			}

			if asyncJobMsg.JobState.State != test.expectedState {
				t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}
		})
	}
}

func TestModifyDb(t *testing.T) {
	modifyDbErr := errors.New("modify DB error")
	testCases := map[string]struct {
		dbInstance             *RDSInstance
		dbAdapter              dbAdapter
		expectedErr            error
		expectedState          base.InstanceState
		expectedAsyncJobStates []base.InstanceState
		password               string
	}{
		"modify DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					modifyDbErr: modifyDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
			},
			dbInstance:    NewRDSInstance(),
			expectedErr:   modifyDbErr,
			expectedState: base.InstanceNotModified,
		},
		"success without read replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds:                  &mockRDSClient{},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        0,
				},
			},
			dbInstance: &RDSInstance{
				dbUtils: &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceInProgress,
		},
		"success with read replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
			},
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
				dbUtils:         &RDSDatabaseUtils{},
			},
			expectedState:          base.InstanceInProgress,
			expectedAsyncJobStates: []base.InstanceState{base.InstanceReady, base.InstanceInProgress},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			responseCode, err := test.dbAdapter.modifyDB(test.dbInstance, test.password, brokerDB)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			if len(test.expectedAsyncJobStates) > 0 {
				asyncJobMsg, err := taskqueue.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.ModifyOp)
				if err != nil {
					t.Fatal(err)
				}

				// The exact database state at this point in the test is non-deterministic, since the database updates
				// are being done in a goroutine. So we test against a set of possible job states
				if !slices.Contains(test.expectedAsyncJobStates, asyncJobMsg.JobState.State) {
					t.Fatalf("expected one of async job states: %+v, got: %s", test.expectedAsyncJobStates, asyncJobMsg.JobState.State)
				}
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
		})
	}
}

func TestPrepareModifyDbInstanceInput(t *testing.T) {
	testErr := errors.New("fail")
	testCases := map[string]struct {
		dbInstance        *RDSInstance
		dbAdapter         *dedicatedDBAdapter
		expectedGroupName string
		expectedErr       error
		expectedParams    *rds.ModifyDBInstanceInput
	}{
		"expect returned group name": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				dbUtils:               &RDSDatabaseUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					customPgroupName: "foobar",
					rds:              &mockRDSClient{},
				},
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
			},
			expectedGroupName: "foobar",
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int64(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int64(14),
				DBParameterGroupName:     aws.String("foobar"),
			},
		},
		"expect error": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				dbUtils:               &RDSDatabaseUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					rds:       &mockRDSClient{},
					returnErr: testErr,
				},
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
			},
			expectedErr: testErr,
		},
		"update password": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				ClearPassword:         "fake-pw",
				dbUtils:               &RDSDatabaseUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				rds: &mockRDSClient{},
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int64(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int64(14),
				MasterUserPassword:       aws.String("fake-pw"),
			},
		},
		"update storage type": {
			dbInstance: &RDSInstance{
				dbUtils:               &RDSDatabaseUtils{},
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				rds: &mockRDSClient{},
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int64(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int64(14),
				StorageType:              aws.String("gp3"),
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := test.dbAdapter.prepareModifyDbInstanceInput(test.dbInstance)
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

func TestDescribeDatbaseInstance(t *testing.T) {
	testCases := map[string]struct {
		dbAdapter        dbAdapter
		expectErr        bool
		database         string
		expectedInstance *rds.DBInstance
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
			},
			database: "foo",
			expectedInstance: &rds.DBInstance{
				DBInstanceStatus: aws.String("available"),
			},
		},
		"error describing database": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database")},
				},
			},
			database:  "foo",
			expectErr: true,
		},
		"no databases found": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{},
						},
					},
				},
			},
			database:  "foo",
			expectErr: true,
		},
		"multiple databases found": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceIdentifier: aws.String("db1"),
								},
								{
									DBInstanceIdentifier: aws.String("db2"),
								},
							},
						},
					},
				},
			},
			database:  "foo",
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			dbInstance, err := test.dbAdapter.describeDatabaseInstance(test.database)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error but received none")
			}
			if diff := deep.Equal(dbInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestBindDBToApp(t *testing.T) {
	testCases := map[string]struct {
		dbAdapter        dbAdapter
		expectErr        bool
		rdsInstance      *RDSInstance
		expectedCreds    map[string]string
		password         string
		expectedInstance *RDSInstance
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
									Endpoint: &rds.Endpoint{
										Address: aws.String("db-address"),
										Port:    aws.Int64(1234),
									},
								},
							},
						},
					},
				},
			},
			rdsInstance: &RDSInstance{
				dbUtils: &MockDbUtils{
					mockFormattedDbName: "db1",
					mockCreds: map[string]string{
						"uri":      "postgres://user-1:fake-pw@db-address:1234/db1",
						"username": "user-1",
						"password": "fake-pw",
						"host":     "db-address",
						"port":     strconv.FormatInt(1234, 10),
						"db_name":  "db1",
						"name":     "db1",
					},
				},
			},
			password: "fake-pw",
			expectedCreds: map[string]string{
				"uri":      "postgres://user-1:fake-pw@db-address:1234/db1",
				"username": "user-1",
				"password": "fake-pw",
				"host":     "db-address",
				"port":     strconv.FormatInt(1234, 10),
				"db_name":  "db1",
				"name":     "db1",
			},
			expectedInstance: &RDSInstance{
				Instance: base.Instance{
					Host:  "db-address",
					Port:  1234,
					State: base.InstanceReady,
				},
			},
		},
		"database not available": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("processing"),
								},
							},
						},
					},
				},
			},
			rdsInstance:      &RDSInstance{},
			expectedInstance: &RDSInstance{},
			password:         "fake-pw",
			expectErr:        true,
		},
		"database has no endpoint": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
								},
							},
						},
					},
				},
			},
			rdsInstance:      &RDSInstance{},
			password:         "fake-pw",
			expectedInstance: &RDSInstance{},
			expectErr:        true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			creds, err := test.dbAdapter.bindDBToApp(test.rdsInstance, test.password)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error but received none")
			}
			if diff := deep.Equal(creds, test.expectedCreds); diff != nil {
				t.Error(diff)
			}
			if diff := deep.Equal(test.rdsInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestWaitForDbDeleted(t *testing.T) {
	dbInstanceNotFoundErr := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))

	testCases := map[string]struct {
		dbInstance            *RDSInstance
		dbAdapter             *dedicatedDBAdapter
		expectedState         base.InstanceState
		expectErr             bool
		expectAsyncJobMessage bool
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
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
		},
		"waits with retries for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("deleting"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("deleting"),
								},
							},
						},
					},
					describeDbInstancesErrs: []error{nil, nil, dbInstanceNotFoundErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        3,
				},
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
		},
		"gives up after maximum retries for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("deleting"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("deleting"),
								},
							},
						},
						{
							DBInstances: []*rds.DBInstance{
								{
									DBInstanceStatus: aws.String("deleting"),
								},
							},
						},
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        3,
				},
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
			expectedState:         base.InstanceNotGone,
			expectErr:             true,
			expectAsyncJobMessage: true,
		},
		"error checking database creation status": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
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
			expectedState:         base.InstanceNotGone,
			expectErr:             true,
			expectAsyncJobMessage: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			err = test.dbAdapter.waitForDbDeleted(brokerDB, base.CreateOp, test.dbInstance, test.dbInstance.Database)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			if test.expectAsyncJobMessage {
				asyncJobMsg, err := taskqueue.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
				if err != nil {
					t.Fatal(err)
				}

				if asyncJobMsg.JobState.State != test.expectedState {
					t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
				}
			}
		})
	}
}
