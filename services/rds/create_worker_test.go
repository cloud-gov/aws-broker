package rds

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/go-test/deep"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

func TestCreateWorker(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

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
			ctx:      context.Background(),
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
				DbType:   "postgres",
			},
			worker: &CreateWorker{
				db: brokerDB,
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
					},
				},
				parameterGroupClient: &mockParameterGroupClient{},
				logger:               logger,
			},
			plan:          &catalog.RDSPlan{},
			expectedState: base.InstanceReady,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			sqlDB, err := brokerDB.DB()
			if err != nil {
				t.Fatal(err)
			}

			workers := river.NewWorkers()
			river.AddWorker(workers, test.worker)
			riverClient, err = jobs.NewClient(test.ctx, brokerDB, test.worker.settings.DbConfig, logger, workers)
			if err != nil {
				log.Fatal(fmt.Errorf("error creating river client: %w", err))
			}

			testWorker := rivertest.NewWorker(t, riversqlite.New(sqlDB), &river.Config{}, test.worker)

			tx := brokerDB.Begin()
			if err := tx.Error; err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			sqlTx := tx.Statement.ConnPool.(*sql.Tx)

			result, err := testWorker.Work(ctx, t, sqlTx, CreateArgs{
				Instance: &RDSInstance{
					Instance: base.Instance{
						Uuid: uuid.NewString(),
					},
					DbType:   "postgres",
					Database: helpers.RandStr(10),
					dbUtils:  &RDSDatabaseUtils{},
				},
				Plan: &catalog.RDSPlan{},
			}, nil)

			if err != nil {
				t.Fatal(err)
			}

			if result.EventKind != river.EventKindJobCompleted {
				t.Fatal("not completed")
			}

			if result.Job.State != rivertype.JobStateCompleted {
				t.Fatal("not completed")
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
				dbUtils:         &RDSDatabaseUtils{},
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
				AllocatedStorage: 10,
				Database:         "db-1",
				BinaryLogFormat:  "ROW",
				DbType:           "mysql",
				dbUtils: &MockDbUtils{
					mockFormattedDbName: "formatted-name",
				},
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
				DBName:                  aws.String("formatted-name"),
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
				AllocatedStorage: 10,
				Database:         "db-1",
				BinaryLogFormat:  "ROW",
				DbType:           "mysql",
				DbVersion:        "8.0",
				dbUtils: &MockDbUtils{
					mockFormattedDbName: "formatted-name",
				},
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
				DBName:                  aws.String("formatted-name"),
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
