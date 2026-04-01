package jobs

import (
	"database/sql"
	"log/slog"
	"runtime"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"gorm.io/gorm"
)

func NewClient(db *gorm.DB, logger *slog.Logger) (*river.Client[*sql.Tx], error) {
	workers := river.NewWorkers()

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	driver := riverdatabasesql.New(sqlDB)
	return river.NewClient(driver, &river.Config{
		JobTimeout: 10 * time.Minute,
		Logger:     logger,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: runtime.GOMAXPROCS(0)}, // Run as many workers as we have CPU cores available.
		},
		Workers: workers,
	})
}
