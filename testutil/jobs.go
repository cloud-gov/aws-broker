package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/cloud-gov/aws-broker/db"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertest"
	"gorm.io/gorm"
)

func GetRiverClient(ctx context.Context, db *gorm.DB, dbConfig *db.DBConfig, workers *river.Workers, logger *slog.Logger) (*river.Client[*sql.Tx], error) {
	dbType := getDbType()

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	switch dbType {
	case "postgres":
		driver := riverdatabasesql.New(sqlDB)
		migrator, err := rivermigrate.New(driver, nil)
		if err != nil {
			return nil, err
		}
		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{})
		if err != nil {
			return nil, err
		}
		riverClient, err := river.NewClient(driver, &river.Config{
			Logger:   logger,
			TestOnly: true,
			Workers:  workers,
		})
		return riverClient, nil
	case "sqlite3":
		driver := riversqlite.New(sqlDB)
		migrator, err := rivermigrate.New(driver, nil)
		if err != nil {
			return nil, err
		}
		_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{})
		if err != nil {
			return nil, err
		}
		riverClient, err := river.NewClient(driver, &river.Config{
			Logger:   logger,
			TestOnly: true,
			Workers:  workers,
		})
		return riverClient, nil
	default:
		return nil, fmt.Errorf("unsupported db type: %s", dbConfig.DbType)
	}
}

func RequireInsertedTx[TArgs river.JobArgs](ctx context.Context, tb testing.TB, tx *sql.Tx, expectedJob TArgs, opts *rivertest.RequireInsertedOpts) (*river.Job[TArgs], error) {
	dbType := getDbType()

	switch dbType {
	case "postgres":
		return rivertest.RequireInsertedTx[*riverdatabasesql.Driver](ctx, tb, tx, expectedJob, opts), nil
	case "sqlite3":
		return rivertest.RequireInsertedTx[*riversqlite.Driver](ctx, tb, tx, expectedJob, opts), nil
	default:
		return nil, fmt.Errorf("unsupported db type: %s", dbType)
	}
}
