package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/services/rds"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
	"gorm.io/gorm"
)

type CustomErrorHandler struct {
	db     *gorm.DB
	logger *slog.Logger
}

func (e *CustomErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	e.logger.Error(fmt.Sprintf("Job kind %s errored", job.Kind), "err", err)
	return nil
}

func (e *CustomErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *river.ErrorHandlerResult {
	e.logger.Error(fmt.Sprintf("Job panicked with: %v, trace: %s", panicVal, trace))
	e.logger.Info(fmt.Sprintf("Cancelling job %s due to panic", job.Kind))
	e.markJobAsFailed(job)
	return &river.ErrorHandlerResult{
		SetCancelled: true,
	}
}

func (e *CustomErrorHandler) markJobAsFailed(job *rivertype.JobRow) {
	var (
		err        error
		instanceID string
	)
	switch job.Kind {
	case rds.ModifyKind:
		args := rds.ModifyArgs{}
		err = json.Unmarshal(job.EncodedArgs, &args)
		if err != nil {
			break
		}
		instanceID = args.Instance.Uuid
		err = asyncmessage.WriteAsyncJobMessage(e.db, args.Instance.ServiceID, instanceID, base.ModifyOp, base.InstanceNotModified, "operation failed due to panic. see logs for more details")
	case rds.CreateKind:
		args := rds.CreateArgs{}
		err = json.Unmarshal(job.EncodedArgs, &args)
		if err != nil {
			break
		}
		instanceID = args.Instance.Uuid
		err = asyncmessage.WriteAsyncJobMessage(e.db, args.Instance.ServiceID, instanceID, base.CreateOp, base.InstanceNotCreated, "operation failed due to panic. see logs for more details")
	case rds.DeleteKind:
		args := rds.CreateArgs{}
		err = json.Unmarshal(job.EncodedArgs, &args)
		if err != nil {
			break
		}
		instanceID = args.Instance.Uuid
		err = asyncmessage.WriteAsyncJobMessage(e.db, args.Instance.ServiceID, instanceID, base.DeleteOp, base.InstanceNotGone, "operation failed due to panic. see logs for more details")
	}

	if err != nil {
		e.logger.Error(fmt.Sprintf("Failed to update status for %s job, instance %s", job.Kind, instanceID), "err", err)
	}
}

func NewClient(ctx context.Context, db *gorm.DB, dbConfig *db.DBConfig, logger *slog.Logger, workers *river.Workers) (*river.Client[*sql.Tx], error) {
	logger.Info("initializing river client")

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	riverConfig := &river.Config{
		ErrorHandler: &CustomErrorHandler{
			db:     db,
			logger: logger,
		},
		JobTimeout: 4 * time.Hour,
		Logger:     logger,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: runtime.GOMAXPROCS(0)}, // Run as many workers as we have CPU cores available.
		},
		Workers: workers,
	}

	switch dbConfig.DbType {
	case "mysql":
	case "postgres":
		driver := riverdatabasesql.New(sqlDB)
		client, err := river.NewClient(driver, riverConfig)
		if err != nil {
			return nil, err
		}
		migrator, err := rivermigrate.New(driver, nil)
		if err != nil {
			return nil, err
		}
		err = runRiverMigration(ctx, migrator, logger)
		if err != nil {
			return nil, err
		}
		return client, nil
	case "sqlite3":
		driver := riversqlite.New(sqlDB)
		client, err := river.NewClient(driver, riverConfig)
		if err != nil {
			return nil, err
		}
		migrator, err := rivermigrate.New(driver, nil)
		if err != nil {
			return nil, err
		}
		err = runRiverMigration(ctx, migrator, logger)
		if err != nil {
			return nil, err
		}
		return client, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.DbType)
	}

	return nil, errors.New("did not create river client")
}

func runRiverMigration(ctx context.Context, migrator *rivermigrate.Migrator[*sql.Tx], logger *slog.Logger) error {
	logger.Info("running migrations for River")
	_, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{})
	return err
}
