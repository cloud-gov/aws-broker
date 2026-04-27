package elasticsearch

import (
	"context"
	"log/slog"

	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	DeleteKind = "elasticsearch-delete"
)

type DeleteArgs struct {
	Instance *ElasticsearchInstance `json:"instance"`
}

func (DeleteArgs) Kind() string { return DeleteKind }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	db         *gorm.DB
	settings   *config.Settings
	opensearch OpensearchClientInterface
	logger     *slog.Logger
}

func NewDeleteWorker(
	db *gorm.DB,
	settings *config.Settings,
	opensearch OpensearchClientInterface,
	logger *slog.Logger,
) *DeleteWorker {
	return &DeleteWorker{
		db:         db,
		settings:   settings,
		opensearch: opensearch,
		logger:     logger,
	}
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return nil
}
