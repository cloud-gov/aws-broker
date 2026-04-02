package jobs

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/cloud-gov/aws-broker/common"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"gorm.io/gorm"
)

func NewClient(db *gorm.DB, dbConfig *common.DBConfig, logger *slog.Logger, workers *river.Workers) (*river.Client[*sql.Tx], error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	riverConfig := &river.Config{
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
		return river.NewClient(driver, riverConfig)
	case "sqlite3":
		driver := riversqlite.New(sqlDB)
		return river.NewClient(driver, riverConfig)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.DbType)
	}

	return nil, errors.New("did not create river client")
}
