package main

import (
	"fmt"
	"net/http"

	"code.cloudfoundry.org/brokerapi/v13"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	brokertags "github.com/cloud-gov/go-broker-tags"

	"log"
	"log/slog"
	"os"

	"github.com/cloud-gov/aws-broker/broker"
	"github.com/cloud-gov/aws-broker/db"
	jobs "github.com/cloud-gov/aws-broker/jobs"
)

func main() {
	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		log.Fatal(err)
	}

	db, err := db.InternalDBInit(settings.DbConfig)
	if err != nil {
		log.Fatal(fmt.Errorf("error initializing database: %s", err))
	}

	asyncJobManager := jobs.NewAsyncJobManager()
	asyncJobManager.Init()

	tagManager, err := brokertags.NewCFTagManager(
		"AWS broker",
		settings.Environment,
		settings.CfApiUrl,
		settings.CfApiClientId,
		settings.CfApiClientSecret,
	)
	if err != nil {
		log.Fatal(err)
	}

	path, _ := os.Getwd()
	c := catalog.InitCatalog(path)

	username := os.Getenv("AUTH_USER")
	password := os.Getenv("AUTH_PASS")

	credentials := brokerapi.BrokerCredentials{
		Username: username,
		Password: password,
	}

	// Create a Text handler that writes to os.Stdout
	handler := slog.NewTextHandler(os.Stdout, nil)

	// Create a new logger with the Text handler
	logger := slog.New(handler)

	serviceBroker := broker.New(
		&settings,
		db,
		c,
		asyncJobManager,
		tagManager,
	)

	brokerAPI := brokerapi.New(serviceBroker, logger, credentials)
	http.Handle("/", brokerAPI)

	http.ListenAndServe(fmt.Sprintf(":%s", settings.Port), nil)
}
