package rds

import (
	"context"
	"database/sql"
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
	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

func TestCreateWorker(t *testing.T) {
	testCases := map[string]struct {
		dbInstance           *RDSInstance
		settings             *config.Settings
		rds                  RDSClientInterface
		parameterGroupClient parameterGroupClient
		expectedState        base.InstanceState
		password             string
		plan                 *catalog.RDSPlan
		expectErr            bool
	}{
		"success without replica": {
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
			password:             helpers.RandStr(10),
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

			var (
				config = &river.Config{}
				driver = riversqlite.New(sqlDB)
				logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
					Level: slog.LevelInfo,
				}))
				worker = &CreateWorker{
					db:                   brokerDB,
					settings:             test.settings,
					rds:                  test.rds,
					parameterGroupClient: test.parameterGroupClient,
					logger:               logger,
				}
				ctx = context.Background()
			)

			workers := river.NewWorkers()
			river.AddWorker(workers, worker)
			riverClient, err = jobs.NewClient(ctx, brokerDB, test.settings.DbConfig, logger, workers)
			if err != nil {
				log.Fatal(fmt.Errorf("error creating river client: %w", err))
			}

			testWorker := rivertest.NewWorker(t, driver, config, worker)

			tx := brokerDB.Begin()
			if err := tx.Error; err != nil {
				t.Fatal(err)
			}

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
