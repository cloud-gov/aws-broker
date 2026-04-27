package redis

import (
	"context"
	"log/slog"

	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	DeleteKind = "elasticache-delete"
)

type DeleteArgs struct {
	Instance *RedisInstance `json:"instance"`
}

func (DeleteArgs) Kind() string { return DeleteKind }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	db          *gorm.DB
	settings    *config.Settings
	elasticache ElasticacheClientInterface
	s3          brokerAws.S3ClientInterface
	logger      *slog.Logger
}

func NewDeleteWorker(
	db *gorm.DB,
	settings *config.Settings,
	elasticache ElasticacheClientInterface,
	s3 brokerAws.S3ClientInterface,
	logger *slog.Logger,
) *DeleteWorker {
	return &DeleteWorker{
		db:          db,
		settings:    settings,
		elasticache: elasticache,
		s3:          s3,
		logger:      logger,
	}
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return nil
}
