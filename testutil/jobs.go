package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/cloud-gov/aws-broker/db"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
	"gorm.io/gorm"
)

func GetRiverClient(ctx context.Context, db *gorm.DB, dbConfig *db.DBConfig, workers *river.Workers, logger *slog.Logger) (*river.Client[*sql.Tx], error) {
	if dbConfig.DbType = os.Getenv("DB_TYPE"); dbConfig.DbType == "" {
		dbConfig.DbType = "sqlite3"
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	switch dbConfig.DbType {
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
