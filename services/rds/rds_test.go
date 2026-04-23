package rds

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-test/deep"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivertest"
	"gorm.io/gorm"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
)

func NewTestDedicatedDBAdapter(ctx context.Context, brokerDB *gorm.DB, s *config.Settings, rdsClient RDSClientInterface, parameterGroupClient parameterGroupClient) *dedicatedDBAdapter {
	logger := slog.New(&mockLogHandler{})

	workers := river.NewWorkers()
	river.AddWorker(workers, NewCreateWorker(brokerDB, s, rdsClient, logger, parameterGroupClient, &mockCredentialUtils{}))
	river.AddWorker(workers, NewModifyWorker(brokerDB, s, rdsClient, logger, parameterGroupClient, &mockCredentialUtils{}))
	river.AddWorker(workers, NewDeleteWorker(brokerDB, s, rdsClient, logger, parameterGroupClient, &mockCredentialUtils{}))

	if s.DbConfig == nil {
		s.DbConfig = &db.DBConfig{
			DbType: "sqlite3",
		}
	}

	riverClient, err := jobs.NewClient(ctx, brokerDB, s.DbConfig, logger, workers)
	if err != nil {
		log.Fatal(fmt.Errorf("error creating river client: %w", err))
	}

	return NewRdsDedicatedDBAdapter(ctx, s, brokerDB, rdsClient, parameterGroupClient, logger, riverClient)
}

func TestCreateDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		dbInstance    *RDSInstance
		dbAdapter     *dedicatedDBAdapter
		expectedErr   error
		expectedState base.InstanceState
		password      string
		plan          *catalog.RDSPlan
	}{
		"success": {
			ctx: context.Background(),
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
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
				},
				&mockParameterGroupClient{},
			),
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
				credentialUtils: &RDSCredentialUtils{},
			},
			expectedState: base.InstanceInProgress,
			plan: &catalog.RDSPlan{
				ServicePlan: domain.ServicePlan{
					ID: uuid.NewString(),
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.dbAdapter.createDB(test.dbInstance, test.plan)

			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			tx := brokerDB.Begin()
			if err := tx.Error; err != nil {
				t.Fatal(err)
			}

			sqlTx := tx.Statement.ConnPool.(*sql.Tx)
			defer tx.Rollback()

			job := rivertest.RequireInsertedTx[*riversqlite.Driver](test.ctx, t, sqlTx, &CreateArgs{}, nil)

			if job.Args.Instance.Uuid != test.dbInstance.Uuid {
				t.Fatal("Did not receive expected RDS instance as create worker argument")
			}

			if job.Args.Plan.ID != test.plan.ID {
				t.Fatal("Did not receive expected RDS plan as create worker argument")
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
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
		ctx           context.Context
		dbInstance    *RDSInstance
		dbAdapter     *dedicatedDBAdapter
		worker        *ModifyWorker
		expectedErr   error
		expectedState base.InstanceState
		plan          *catalog.RDSPlan
	}{
		"success": {
			ctx: context.Background(),
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
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
				&mockParameterGroupClient{},
			),
			worker: &ModifyWorker{
				db: brokerDB,
				settings: &config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				rds: &mockRDSClient{
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
				parameterGroupClient: &mockParameterGroupClient{},
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
				credentialUtils: &RDSCredentialUtils{},
			},
			expectedState: base.InstanceInProgress,
			plan:          &catalog.RDSPlan{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {

			responseCode, err := test.dbAdapter.modifyDB(test.dbInstance, test.plan)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}

			tx := brokerDB.Begin()
			if err := tx.Error; err != nil {
				t.Fatal(err)
			}

			sqlTx := tx.Statement.ConnPool.(*sql.Tx)
			defer tx.Rollback()

			job := rivertest.RequireInsertedTx[*riversqlite.Driver](test.ctx, t, sqlTx, &ModifyArgs{}, nil)

			if job.Args.Instance.Uuid != test.dbInstance.Uuid {
				t.Fatal("Did not receive expected RDS instance as create worker argument")
			}

			if job.Args.Plan.ID != test.plan.ID {
				t.Fatal("Did not receive expected RDS plan as create worker argument")
			}

			if responseCode != test.expectedState {
				t.Errorf("expected response: %s, got: %s", test.expectedState, responseCode)
			}
		})
	}
}

func TestDescribeDatbaseInstance(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		dbAdapter        dbAdapter
		expectErr        bool
		database         string
		expectedInstance *rdsTypes.DBInstance
	}{
		"success": {
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
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
				},
				&mockParameterGroupClient{},
			),
			database: "foo",
			expectedInstance: &rdsTypes.DBInstance{
				DBInstanceStatus: aws.String("available"),
			},
		},
		"error describing database": {
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database")},
				},
				&mockParameterGroupClient{},
			),
			database:  "foo",
			expectErr: true,
		},
		"no databases found": {
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{},
						},
					},
				},
				&mockParameterGroupClient{},
			),
			database:  "foo",
			expectErr: true,
		},
		"multiple databases found": {
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
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
				&mockParameterGroupClient{},
			),
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
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("available"),
									Endpoint: &rdsTypes.Endpoint{
										Address: aws.String("db-address"),
										Port:    aws.Int32(1234),
									},
								},
							},
						},
					},
				},
				&mockParameterGroupClient{},
			),
			rdsInstance: &RDSInstance{
				credentialUtils: &mockCredentialUtils{
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
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
				&mockRDSClient{
					describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
						{
							DBInstances: []rdsTypes.DBInstance{
								{
									DBInstanceStatus: aws.String("processing"),
								},
							},
						},
					},
				},
				&mockParameterGroupClient{},
			),
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
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{},
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
				},
				&mockParameterGroupClient{},
			),
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
			err := brokerDB.Create(test.rdsInstance).Error
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

func TestDeleteDb(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	dbInstanceNotFoundErr := &rdsTypes.DBInstanceNotFoundFault{
		Message: aws.String("operation failed"),
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
			dbAdapter: NewTestDedicatedDBAdapter(
				context.Background(),
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr},
				},
				&mockParameterGroupClient{},
			),
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

func TestGetRetryMultiplier(t *testing.T) {
	testCases := map[string]struct {
		storageSize        int64
		defaultMaxRetries  int64
		expectedMultiplier int64
	}{
		"storage = 0": {
			storageSize:        0,
			defaultMaxRetries:  1,
			expectedMultiplier: 1,
		},
		"storage = 1": {
			storageSize:        1,
			defaultMaxRetries:  1,
			expectedMultiplier: 1,
		},
		"storage = 201": {
			storageSize:        201,
			defaultMaxRetries:  1,
			expectedMultiplier: 2,
		},
		"storage = 1000": {
			storageSize:        1000,
			defaultMaxRetries:  1,
			expectedMultiplier: 5,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			multiplier := getRetryMultiplier(test.storageSize)
			if multiplier != test.expectedMultiplier {
				t.Fatalf("expected %d, got %d", test.expectedMultiplier, multiplier)
			}
		})
	}
}

func TestGetPollAwsMaxWaitTime(t *testing.T) {
	testCases := map[string]struct {
		storageSize         int64
		defaultMaxRetries   int64
		expectedMaxRetries  int64
		initialMaxWaitTime  time.Duration
		expectedMaxWaitTime time.Duration
	}{
		"storage = 0": {
			storageSize:         0,
			expectedMaxRetries:  1,
			initialMaxWaitTime:  1 * time.Second,
			expectedMaxWaitTime: 1 * time.Second,
		},
		"storage = 1": {
			storageSize:         1,
			expectedMaxRetries:  1,
			initialMaxWaitTime:  1 * time.Second,
			expectedMaxWaitTime: 1 * time.Second,
		},
		"storage = 201": {
			storageSize:         201,
			expectedMaxRetries:  2,
			initialMaxWaitTime:  1 * time.Second,
			expectedMaxWaitTime: 2 * time.Second,
		},
		"storage = 1000": {
			storageSize:         1000,
			expectedMaxRetries:  5,
			initialMaxWaitTime:  1 * time.Second,
			expectedMaxWaitTime: 5 * time.Second,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			maxWaitTime := getPollAwsMaxWaitTime(test.storageSize, test.initialMaxWaitTime)
			if maxWaitTime != test.expectedMaxWaitTime {
				t.Fatalf("expected %d, got %d", test.expectedMaxRetries, maxWaitTime)
			}
		})
	}
}

func TestGetPollAwsMaxRetries(t *testing.T) {
	testCases := map[string]struct {
		storageSize       int64
		defaultMaxRetries int64
		expectedRetries   int
	}{
		"storage = 0": {
			storageSize:       0,
			defaultMaxRetries: 1,
			expectedRetries:   1,
		},
		"storage = 1": {
			storageSize:       1,
			defaultMaxRetries: 1,
			expectedRetries:   1,
		},
		"storage = 201": {
			storageSize:       201,
			defaultMaxRetries: 1,
			expectedRetries:   2,
		},
		"storage = 1000": {
			storageSize:       1000,
			defaultMaxRetries: 1,
			expectedRetries:   5,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			retries := getPollAwsMaxRetries(test.storageSize, test.defaultMaxRetries)
			if retries != test.expectedRetries {
				t.Fatalf("expected %d, got %d", test.expectedRetries, retries)
			}
		})
	}
}
