package main

import (
	"fmt"
	"io"
	"net/http"

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

func run(out io.Writer) error {
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
	)
	brokerAPI := brokerapi.New(serviceBroker, logger, credentials)

	logger.Debug("run: starting web server")
	http.Handle("/", brokerAPI)
	http.ListenAndServe(fmt.Sprintf(":%s", settings.Port), nil)

	return nil
}

func main() {
	err := run(os.Stdout)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}
