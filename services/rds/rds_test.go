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
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/go-test/deep"
	"github.com/google/uuid"
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
		"handles optional params": {
			dbInstance: &RDSInstance{
				AllocatedStorage: 10,
				Database:         "db-1",
				BinaryLogFormat:  "ROW",
				DbType:           "mysql",
				DbVersion:        "8.0",
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
				LicenseModel:          "foo",
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
				EngineVersion:        aws.String("8.0"),
				LicenseModel:         aws.String("foo"),
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

func TestAsyncCreateDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	createDbErr := errors.New("create DB error")
	testCases := map[string]struct {
		dbInstance    *RDSInstance
		dbAdapter     *dedicatedDBAdapter
		expectedState base.InstanceState
		password      string
	}{
		"error creating input params": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					createDbErr: createDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{
					returnErr: errors.New("failed"),
				},
				db: brokerDB,
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
			expectedState: base.InstanceNotCreated,
		},
		"create DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					createDbErr: createDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
				db:                   brokerDB,
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
			expectedState: base.InstanceNotCreated,
		},
		"error waiting for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("fail")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				db:                   brokerDB,
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
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState: base.InstanceReady,
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
				db: brokerDB,
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
			expectedState: base.InstanceReady,
		},
		"error creating replica": {
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
					createDBInstanceReadReplicaErr: errors.New("fail"),
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState: base.InstanceNotCreated,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			test.dbAdapter.asyncCreateDB(test.dbInstance, test.password)

			asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
			if err != nil {
				t.Fatal(err)
			}

			if test.expectedState != asyncJobMsg.JobState.State {
				t.Fatalf("expected async job state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}
		})
	}
}

func TestCreateDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		dbInstance             *RDSInstance
		dbAdapter              *dedicatedDBAdapter
		expectedErr            error
		expectedState          base.InstanceState
		password               string
		expectedAsyncJobStates []base.InstanceState
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
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			responseCode, err := test.dbAdapter.createDB(test.dbInstance, test.password)

			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			if len(test.expectedAsyncJobStates) > 0 {
				asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
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
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

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
				db: brokerDB,
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
				db: brokerDB,
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
				db: brokerDB,
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
				db: brokerDB,
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
			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			err = test.dbAdapter.waitForDbReady(base.CreateOp, test.dbInstance, test.dbInstance.Database)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			if test.expectAsyncJobMessage {
				asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
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
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		dbInstance    *RDSInstance
		dbAdapter     *dedicatedDBAdapter
		expectedState base.InstanceState
		expectErr     bool
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
				db: brokerDB,
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
			expectedState: base.InstanceInProgress,
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
				db: brokerDB,
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
			expectErr:     true,
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
				db: brokerDB,
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
			expectErr:     true,
		},
		"error adding tags": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					addTagsToResourceErr: errors.New("error adding tags to read replica"),
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
				db: brokerDB,
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
			expectErr:     true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.dbAdapter.waitAndCreateDBReadReplica(base.CreateOp, test.dbInstance)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
			if err != nil {
				t.Fatal(err)
			}

			if asyncJobMsg.JobState.State != test.expectedState {
				t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}
		})
	}
}

func TestAsyncModifyDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	modifyDbErr := errors.New("modify DB error")
	dbInstanceNotFoundErr := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))

	testCases := map[string]struct {
		dbInstance         *RDSInstance
		dbAdapter          *dedicatedDBAdapter
		expectedState      base.InstanceState
		expectedDbInstance *RDSInstance
	}{
		"error preparing modify input": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					modifyDbErrs: []error{modifyDbErr},
				},
				parameterGroupClient: &mockParameterGroupClient{
					returnErr: errors.New("fail"),
				},
				db: brokerDB,
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
			expectedState: base.InstanceNotModified,
		},
		"modify primary DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					modifyDbErrs: []error{modifyDbErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				db:                   brokerDB,
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
			expectedState: base.InstanceNotModified,
		},
		"error waiting for database to be ready": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("fail")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				db:                   brokerDB,
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
			expectedState: base.InstanceNotModified,
		},
		"success without read replica": {
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
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
				Database: "db-1",
				dbUtils:  &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceReady,
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
					},
					Uuid: "uuid-1",
				},
				Database: "db-1",
				dbUtils:  &RDSDatabaseUtils{},
			},
		},
		"success with adding read replica": {
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
				db: brokerDB,
			},
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
				dbUtils:         &RDSDatabaseUtils{},
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
				dbUtils:         &RDSDatabaseUtils{},
			},
		},
		"error modifying read replica": {
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
					modifyDbErrs: []error{nil, modifyDbErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "db-replica",
				dbUtils:         &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceNotModified,
		},
		"error creating read replica": {
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
					createDBInstanceReadReplicaErr: errors.New("error creating read replica"),
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState: base.InstanceNotModified,
		},
		"success with deleting read replica": {
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
					describeDbInstancesErrs: []error{nil, dbInstanceNotFoundErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
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
				dbUtils:           &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceReady,
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-3",
					},
					Uuid: "uuid-3",
				},
				Database: "db-3",
				dbUtils:  &RDSDatabaseUtils{},
			},
		},
		"error updating read replica tags": {
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
					addTagsToResourceErr: errors.New("error updating tags"),
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: "db-replica",
				dbUtils:         &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceNotModified,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			test.dbAdapter.asyncModifyDb(test.dbInstance)

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

func TestModifyDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		dbInstance             *RDSInstance
		dbAdapter              dbAdapter
		expectedErr            error
		expectedState          base.InstanceState
		expectedAsyncJobStates []base.InstanceState
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
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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

			responseCode, err := test.dbAdapter.modifyDB(test.dbInstance)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			if len(test.expectedAsyncJobStates) > 0 {
				asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.ModifyOp)
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
			params, err := test.dbAdapter.prepareModifyDbInstanceInput(test.dbInstance, test.dbInstance.Database)
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
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

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
				db: brokerDB,
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
				Instance: base.Instance{
					Uuid: uuid.NewString(),
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
				db: brokerDB,
			},
			rdsInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
				},
			},
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
				db: brokerDB,
			},
			rdsInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
				},
			},
			password:         "fake-pw",
			expectedInstance: &RDSInstance{},
			expectErr:        true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = brokerDB.Create(test.rdsInstance).Error
			if err != nil {
				t.Fatal(err)
			}

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

			test.expectedInstance.Uuid = test.rdsInstance.Uuid

			if diff := deep.Equal(test.rdsInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}

			dbInstanceRecord := RDSInstance{}
			brokerDB.Where("uuid = ?", test.rdsInstance.Uuid).First(&dbInstanceRecord)
			if diff := deep.Equal(&dbInstanceRecord, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestWaitForDbDeleted(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

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
				db: brokerDB,
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
				db: brokerDB,
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
				db: brokerDB,
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
				db: brokerDB,
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
			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			err = test.dbAdapter.waitForDbDeleted(base.CreateOp, test.dbInstance, test.dbInstance.Database)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			if test.expectAsyncJobMessage {
				asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
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

func TestAsyncDeleteDB(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	dbInstanceNotFoundErr := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))

	testCases := map[string]struct {
		dbInstance          *RDSInstance
		dbAdapter           *dedicatedDBAdapter
		expectedState       base.InstanceState
		expectedRecordCount int64
	}{
		"success without replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState: base.InstanceGone,
		},
		"success with replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr, dbInstanceNotFoundErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState: base.InstanceGone,
		},
		"error checking database status": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState:       base.InstanceInProgress,
			expectedRecordCount: 1,
		},
		"error checking replica database status": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState:       base.InstanceInProgress,
			expectedRecordCount: 1,
		},
		"error deleting database": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					deleteDbInstancesErrs: []error{errors.New("failed to delete database")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState:       base.InstanceInProgress,
			expectedRecordCount: 1,
		},
		"error deleting replica database": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					deleteDbInstancesErrs: []error{errors.New("failed to delete database")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState:       base.InstanceInProgress,
			expectedRecordCount: 1,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = brokerDB.Create(test.dbInstance).Error
			if err != nil {
				t.Fatal(err)
			}

			var count int64
			brokerDB.Where("uuid = ?", test.dbInstance.Uuid).First(test.dbInstance).Count(&count)
			if count == 0 {
				t.Fatal("The instance should be in the DB")
			}

			// do not invoke in a goroutine so that we can guarantee it has finished to observe its results
			test.dbAdapter.asyncDeleteDB(test.dbInstance)

			asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.DeleteOp)
			if err != nil {
				t.Fatal(err)
			}

			if asyncJobMsg.JobState.State != test.expectedState {
				t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}

			brokerDB.Where("uuid = ?", test.dbInstance.Uuid).First(test.dbInstance).Count(&count)
			if count != test.expectedRecordCount {
				t.Fatalf("expected %d records, found %d", test.expectedRecordCount, count)
			}
		})
	}
}

func TestDeleteDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	dbInstanceNotFoundErr := awserr.New(rds.ErrCodeDBInstanceNotFoundFault, "message", errors.New("operation failed"))

	testCases := map[string]struct {
		dbInstance             *RDSInstance
		dbAdapter              *dedicatedDBAdapter
		expectedErr            error
		expectedState          base.InstanceState
		password               string
		expectedAsyncJobStates []base.InstanceState
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        1,
				},
				db: brokerDB,
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
			expectedState:          base.InstanceInProgress,
			expectedAsyncJobStates: []base.InstanceState{base.InstanceInProgress, base.InstanceGone},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.dbAdapter.deleteDB(test.dbInstance)

			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			if len(test.expectedAsyncJobStates) > 0 {
				asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.DeleteOp)
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
