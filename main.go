package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/signal"

	"code.cloudfoundry.org/brokerapi/v13"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	brokertags "github.com/cloud-gov/go-broker-tags"

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

	logger.Debug("run :initializing database")
	db, err := db.InternalDBInit(settings.DbConfig)
	if err != nil {
		return fmt.Errorf("error initializing database: %s", err)
	}

	logger.Debug("run: initializing River workers and client")
	riverClient, err := jobs.NewClient(db, logger)
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
		&settings,
		db,
		c,
		asyncJobManager,
		tagManager,
		riverClient,
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
