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
	"gorm.io/gorm"
)

func TestWaitForDbReady(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx                   context.Context
		dbInstance            *RDSInstance
		expectedState         base.InstanceState
		expectErr             bool
		expectAsyncJobMessage bool
		db                    *gorm.DB
		settings              *config.Settings
		rds                   RDSClientInterface
		logger                *slog.Logger
	}{
		"success": {
			ctx: t.Context(),
			db:  brokerDB,
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
				},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),

			db: brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 10 * time.Millisecond,
			},
			rds: &mockRDSClient{
				describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("creating"),
							},
						},
					},
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("creating"),
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
			logger: slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),

			db: brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 3 * time.Millisecond,
			},
			rds: &mockRDSClient{
				describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("creating"),
							},
						},
					},
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("creating"),
							},
						},
					},
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("creating"),
							},
						},
					},
				},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),
			db:  brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 3 * time.Millisecond,
			},
			rds: &mockRDSClient{
				describeDbInstancesErrs: []error{errors.New("error describing database instances")},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			err := waitForDbReady(test.ctx, test.db, test.settings, test.rds, test.logger, base.CreateOp, test.dbInstance, test.dbInstance.Database)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			if test.expectAsyncJobMessage {
				asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
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

func TestWaitForDbDeleted(t *testing.T) {
	dbInstanceNotFoundErr := &rdsTypes.DBInstanceNotFoundFault{
		Message: aws.String("operation failed"),
	}
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
		db                    *gorm.DB
		settings              *config.Settings
		rdsClient             RDSClientInterface
		parameterGroupClient  parameterGroupClient
		ctx                   context.Context
		logger                *slog.Logger
	}{
		"success": {
			ctx: t.Context(),
			db:  brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 1 * time.Millisecond,
			},
			rdsClient: &mockRDSClient{
				describeDbInstancesErrs: []error{dbInstanceNotFoundErr},
			},
			parameterGroupClient: &mockParameterGroupClient{},
			logger:               slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),
			db:  brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 5 * time.Millisecond,
			},
			rdsClient: &mockRDSClient{
				describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("deleting"),
							},
						},
					},
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("deleting"),
							},
						},
					},
				},
				describeDbInstancesErrs: []error{nil, nil, dbInstanceNotFoundErr},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),
			db:  brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 3 * time.Millisecond,
			},
			rdsClient: &mockRDSClient{
				describeDbInstancesResults: []*rds.DescribeDBInstancesOutput{
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("deleting"),
							},
						},
					},
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("deleting"),
							},
						},
					},
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("deleting"),
							},
						},
					},
				},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			ctx: t.Context(),
			db:  brokerDB,
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 1 * time.Millisecond,
			},
			rdsClient: &mockRDSClient{
				describeDbInstancesErrs: []error{errors.New("error describing database instances")},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			err := waitForDbDeleted(test.ctx, test.db, test.settings, test.rdsClient, test.logger, base.CreateOp, test.dbInstance, test.dbInstance.Database)
			if !test.expectErr && err != nil {
				t.Fatal(err)
			}

			if test.expectAsyncJobMessage {
				asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.CreateOp)
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
		ctx           context.Context
		settings      *config.Settings
		rds           *mockRDSClient
		logger        *slog.Logger
		dbInstance    *RDSInstance
		expectedState base.InstanceState
		expectErr     bool
		plan          *catalog.RDSPlan
	}{
		"success": {
			ctx: t.Context(),
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 1 * time.Millisecond,
				DbConfig: &db.DBConfig{
					DbType: "sqlite3",
				},
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
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("available"),
							},
						},
					},
				},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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

			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 1 * time.Millisecond,
				DbConfig: &db.DBConfig{
					DbType: "sqlite3",
				},
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
				},
				describeDbInstancesErrs: []error{nil, errors.New("error describing database instances")},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 1 * time.Millisecond,
				DbConfig: &db.DBConfig{
					DbType: "sqlite3",
				},
			},
			rds: &mockRDSClient{
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
			logger: slog.New(&testutil.MockLogHandler{}),

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
			settings: &config.Settings{
				PollAwsMinDelay:    1 * time.Millisecond,
				PollAwsMaxDuration: 3 * time.Millisecond,
				DbConfig: &db.DBConfig{
					DbType: "sqlite3",
				},
			},
			rds: &mockRDSClient{
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
					{
						DBInstances: []rdsTypes.DBInstance{
							{
								DBInstanceStatus: aws.String("available"),
							},
						},
					},
				},
			},
			logger: slog.New(&testutil.MockLogHandler{}),
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
			err := waitAndCreateDBReadReplica(test.ctx, brokerDB, test.settings, test.rds, test.logger, base.CreateOp, test.dbInstance, test.plan)
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
