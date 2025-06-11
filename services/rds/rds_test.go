package rds

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
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
		dbInstance          *RDSInstance
		dbAdapter           dbAdapter
		expectedErr         error
		expectedState       base.InstanceState
		password            string
		queueManager        taskqueue.QueueManager
		expectedAsyncStates []base.InstanceState
	}{
		"create DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					createDbErr: createDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
			},
			dbInstance:    NewRDSInstance(),
			expectedErr:   createDbErr,
			expectedState: base.InstanceNotCreated,
			queueManager:  &mockQueueManager{},
		},
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					describeDBInstancesResponses: []*string{aws.String("available")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
			},
			dbInstance: &RDSInstance{
				ReplicaDatabase: "replica",
				dbUtils:         &RDSDatabaseUtils{},
			},
			expectedState: base.InstanceInProgress,
			queueManager: &mockQueueManager{
				jobChan: make(chan taskqueue.AsyncJobMsg),
			},
			expectedAsyncStates: []base.InstanceState{base.InstanceInProgress, base.InstanceInProgress, base.InstanceReady},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.dbAdapter.createDB(test.dbInstance, test.password, test.queueManager)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
			if len(test.expectedAsyncStates) > 0 {
				if mockQueueManager, ok := test.queueManager.(*mockQueueManager); ok {
					counter := 0
					for jobMsg := range mockQueueManager.jobChan {
						if jobMsg.JobState.State != test.expectedAsyncStates[counter] {
							t.Fatalf("expected state: %s, got: %s", test.expectedAsyncStates[counter], jobMsg.JobState.State)
						}
						counter++
					}
				}
			}
		})
	}
}

func TestWaitAndCreateDBReadReplica(t *testing.T) {
	testCases := map[string]struct {
		dbInstance     *RDSInstance
		dbAdapter      dbAdapter
		expectedErr    error
		expectedStates []base.InstanceState
		password       string
		jobchan        chan taskqueue.AsyncJobMsg
	}{
		"success": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					describeDBInstancesResponses: []*string{aws.String("available")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
			},
			dbInstance:     NewRDSInstance(),
			expectedStates: []base.InstanceState{base.InstanceInProgress, base.InstanceInProgress, base.InstanceReady},
			jobchan:        make(chan taskqueue.AsyncJobMsg),
		},
		"waits with retries for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					describeDBInstancesResponses: []*string{aws.String("creating"), aws.String("creating"), aws.String("available")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
			},
			dbInstance:     NewRDSInstance(),
			expectedStates: []base.InstanceState{base.InstanceInProgress, base.InstanceInProgress, base.InstanceInProgress, base.InstanceInProgress, base.InstanceReady},
			jobchan:        make(chan taskqueue.AsyncJobMsg),
		},
		"gives up after maximum retries for database creation": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					describeDBInstancesResponses: []*string{aws.String("creating"), aws.String("creating"), aws.String("creating"), aws.String("creating"), aws.String("creating")},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
			},
			dbInstance:     NewRDSInstance(),
			expectedStates: []base.InstanceState{base.InstanceInProgress, base.InstanceInProgress, base.InstanceInProgress, base.InstanceInProgress, base.InstanceInProgress, base.InstanceInProgress, base.InstanceNotCreated},
			jobchan:        make(chan taskqueue.AsyncJobMsg),
		},
		"error checking database creation status": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					describeDBInstancesErr: errors.New("error describing database instances"),
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
			},
			dbInstance:     NewRDSInstance(),
			expectedStates: []base.InstanceState{base.InstanceInProgress, base.InstanceNotCreated},
			jobchan:        make(chan taskqueue.AsyncJobMsg),
		},
		"error creating database replica": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					describeDBInstancesResponses:   []*string{aws.String("available")},
					createDBInstanceReadReplicaErr: errors.New("error creating database instance read replica"),
				},
				parameterGroupClient: &mockParameterGroupClient{},
				settings: config.Settings{
					PollAwsRetryDelaySeconds: 0,
					PollAwsMaxRetries:        5,
				},
			},
			dbInstance:     NewRDSInstance(),
			expectedStates: []base.InstanceState{base.InstanceInProgress, base.InstanceInProgress, base.InstanceNotCreated},
			jobchan:        make(chan taskqueue.AsyncJobMsg),
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			go test.dbAdapter.waitAndCreateDBReadReplica(test.dbInstance, test.jobchan)
			counter := 0
			for jobMsg := range test.jobchan {
				if jobMsg.JobState.State != test.expectedStates[counter] {
					t.Fatalf("expected state: %s, got: %s", test.expectedStates[counter], jobMsg.JobState.State)
				}
				counter++
			}
		})
	}
}

func TestModifyDb(t *testing.T) {
	modifyDbErr := errors.New("modify DB error")
	testCases := map[string]struct {
		dbInstance           *RDSInstance
		dbAdapter            dbAdapter
		expectedErr          error
		expectedResponseCode base.InstanceState
		password             string
	}{
		"modify DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					modifyDbErr: modifyDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
			},
			dbInstance:           NewRDSInstance(),
			expectedErr:          modifyDbErr,
			expectedResponseCode: base.InstanceNotModified,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.dbAdapter.modifyDB(test.dbInstance, test.password)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if responseCode != test.expectedResponseCode {
				t.Errorf("expected response: %s, got: %s", test.expectedResponseCode, responseCode)
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

type mockRDSClient struct {
	rdsiface.RDSAPI

	dbEngineVersions                    []*rds.DBEngineVersion
	describeEngVersionsErr              error
	describeDbParamsErr                 error
	createDbParamGroupErr               error
	modifyDbParamGroupErr               error
	describeEngineDefaultParamsResults  []*rds.DescribeEngineDefaultParametersOutput
	describeEngineDefaultParamsErr      error
	describeEngineDefaultParamsNumPages int
	describeEngineDefaultParamsPageNum  int
	describeDbParamsResults             []*rds.DescribeDBParametersOutput
	describeDbParamsNumPages            int
	describeDbParamsPageNum             int
	describeDbInstancesResults          *rds.DescribeDBInstancesOutput
	describeDbInstancesErr              error
}

func (m mockRDSClient) DescribeDBParameters(*rds.DescribeDBParametersInput) (*rds.DescribeDBParametersOutput, error) {
	if m.describeDbParamsErr != nil {
		return nil, m.describeDbParamsErr
	}
	return nil, nil
}

func (m mockRDSClient) DescribeDBEngineVersions(*rds.DescribeDBEngineVersionsInput) (*rds.DescribeDBEngineVersionsOutput, error) {
	if m.describeEngVersionsErr != nil {
		return nil, m.describeEngVersionsErr
	}
	if m.dbEngineVersions != nil {
		return &rds.DescribeDBEngineVersionsOutput{
			DBEngineVersions: m.dbEngineVersions,
		}, nil
	}
	return nil, nil
}

func (m mockRDSClient) CreateDBParameterGroup(*rds.CreateDBParameterGroupInput) (*rds.CreateDBParameterGroupOutput, error) {
	if m.createDbParamGroupErr != nil {
		return nil, m.createDbParamGroupErr
	}
	return nil, nil
}

func (m mockRDSClient) ModifyDBParameterGroup(*rds.ModifyDBParameterGroupInput) (*rds.DBParameterGroupNameMessage, error) {
	if m.modifyDbParamGroupErr != nil {
		return nil, m.modifyDbParamGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) DescribeEngineDefaultParametersPages(input *rds.DescribeEngineDefaultParametersInput, fn func(*rds.DescribeEngineDefaultParametersOutput, bool) bool) error {
	if m.describeEngineDefaultParamsErr != nil {
		return m.describeEngineDefaultParamsErr
	}
	shouldContinue := true
	for shouldContinue {
		output := m.describeEngineDefaultParamsResults[m.describeEngineDefaultParamsPageNum]
		m.describeEngineDefaultParamsPageNum++
		lastPage := m.describeEngineDefaultParamsPageNum == m.describeEngineDefaultParamsNumPages
		shouldContinue = fn(output, lastPage)
	}
	return nil
}

func (m *mockRDSClient) DescribeDBParametersPages(input *rds.DescribeDBParametersInput, fn func(*rds.DescribeDBParametersOutput, bool) bool) error {
	if m.describeDbParamsErr != nil {
		return m.describeDbParamsErr
	}
	shouldContinue := true
	for shouldContinue {
		output := m.describeDbParamsResults[m.describeDbParamsPageNum]
		m.describeDbParamsPageNum++
		lastPage := m.describeDbParamsPageNum == m.describeDbParamsNumPages
		shouldContinue = fn(output, lastPage)
	}
	return nil
}

func (m *mockRDSClient) DescribeDBInstances(input *rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error) {
	if m.describeDbInstancesErr != nil {
		return nil, m.describeDbInstancesErr
	}
	return m.describeDbInstancesResults, nil
}
