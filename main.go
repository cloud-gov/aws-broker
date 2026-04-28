package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/signal"

	"code.cloudfoundry.org/brokerapi/v13"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	awsRds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	"github.com/cloud-gov/aws-broker/services/rds"
	"github.com/cloud-gov/aws-broker/services/redis"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/riverqueue/river"

	"log/slog"
	"os"

	"github.com/cloud-gov/aws-broker/broker"
	"github.com/cloud-gov/aws-broker/db"
	jobs "github.com/cloud-gov/aws-broker/jobs"
)

func run(ctx context.Context, out io.Writer) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		return err
	}

	// Create a Text handler that writes to os.Stdout
	handler := slog.NewTextHandler(out, &slog.HandlerOptions{
		Level: settings.LogLevel,
	})

	// Create a new logger with the Text handler
	logger := slog.New(handler)

	logger.Debug("run: initializing database")
	db, err := db.DBInit(settings.DbConfig)
	if err != nil {
		return fmt.Errorf("error initializing database: %s", err)
	}

	logger.Debug("run: Migrating GORM models")
	// Automigrate!
	err = db.AutoMigrate(&rds.RDSInstance{}, &redis.RedisInstance{}, &elasticsearch.ElasticsearchInstance{}, &base.Instance{}, &asyncmessage.AsyncJobMsg{}) // Add all your models here to help setup the database tables
	if err != nil {
		return fmt.Errorf("error migrating GORM models: %s", err)
	}
	logger.Debug("run: Migrated GORM models")

	cfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithRegion(settings.Region),
	)
	if err != nil {
		return fmt.Errorf("error loading AWS config: %s", err)
	}

	logger.Debug("run: initializing River workers and client")
	workers := river.NewWorkers()

	// RDS workers
	rdsClient := awsRds.NewFromConfig(cfg)
	parameterGroupClient := rds.NewAwsParameterGroupClient(ctx, rdsClient, &settings)
	credentialUtils := &rds.RDSCredentialUtils{}
	river.AddWorker(workers, rds.NewCreateWorker(
		db, &settings, rdsClient, logger, parameterGroupClient, credentialUtils,
	))
	river.AddWorker(workers, rds.NewModifyWorker(
		db, &settings, rdsClient, logger, parameterGroupClient, credentialUtils,
	))
	river.AddWorker(workers, rds.NewDeleteWorker(
		db, &settings, rdsClient, logger, parameterGroupClient, credentialUtils,
	))

	// ElastiCache workers
	elasticacheClient := elasticache.NewFromConfig(cfg)
	s3 := s3.NewFromConfig(cfg)
	river.AddWorker(workers, redis.NewModifyWorker(
		db, &settings, elasticacheClient, logger,
	))
	river.AddWorker(workers, redis.NewDeleteWorker(
		db, &settings, elasticacheClient, s3, logger,
	))

	// OpenSearch workers
	opensearch := opensearch.NewFromConfig(cfg)
	iamSvc := iam.NewFromConfig(cfg)
	river.AddWorker(workers, elasticsearch.NewDeleteWorker(
		db, &settings, opensearch, iamSvc, s3, logger,
	))

	riverClient, err := jobs.NewClient(ctx, db, settings.DbConfig, logger, workers)
	if err != nil {
		return fmt.Errorf("error creating river client: %w", err)
	}

	logger.Debug("run: starting River server")
	if err = riverClient.Start(ctx); err != nil {
		return fmt.Errorf("error starting river client: %w", err)
	}

	asyncJobManager := jobs.NewAsyncJobManager()
	asyncJobManager.Init()

	logger.Debug("run: initializing tags manager")
	tagManager, err := brokertags.NewCFTagManager(
		"AWS broker",
		settings.Environment,
		settings.CfApiUrl,
		settings.CfApiClientId,
		settings.CfApiClientSecret,
	)
	if err != nil {
		return err
	}

	path, _ := os.Getwd()
	c := catalog.InitCatalog(path)

	username := os.Getenv("AUTH_USER")
	password := os.Getenv("AUTH_PASS")

	credentials := brokerapi.BrokerCredentials{
		Username: username,
		Password: password,
	}

	logger.Debug("run: initializing broker")
	serviceBroker := broker.New(
		ctx,
		&settings,
		db,
		c,
		asyncJobManager,
		tagManager,
		riverClient,
		logger,
	)
	brokerAPI := brokerapi.New(serviceBroker, logger, credentials)

	logger.Debug("run: starting web server")
	http.Handle("/", brokerAPI)
	http.ListenAndServe(fmt.Sprintf(":%s", settings.Port), nil)

	return nil
}

func main() {
	ctx := context.Background()
	err := run(ctx, os.Stdout)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
