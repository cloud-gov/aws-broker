package main

import (
	"fmt"
	"net/http"

	"code.cloudfoundry.org/brokerapi/v13"
	"github.com/cloud-gov/aws-broker/config"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"
	"gorm.io/gorm"

	"log"
	"log/slog"
	"os"

	"github.com/cloud-gov/aws-broker/broker"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/db"
	jobs "github.com/cloud-gov/aws-broker/jobs"
)

func main() {
	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		log.Fatal(err)
	}

	DB, err := db.InternalDBInit(settings.DbConfig)
	if err != nil {
		log.Fatal(fmt.Errorf("There was an error with the DB. Error: " + err.Error()))
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

	serviceBroker := broker.New(
		tagManager,
	)

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

	brokerAPI := brokerapi.New(serviceBroker, logger, credentials)
	http.Handle("/", brokerAPI)

	// TODO: make port not hard-coded?
	port := "3000"

	http.ListenAndServe(":"+port, nil)

	// Try to connect and create the app.
	// if m := App(&settings, DB, asyncJobManager, tagManager); m != nil {
	// 	log.Println("Starting app...")
	// 	m.Run()
	// } else {
	// 	log.Println("Unable to setup application. Exiting...")
	// }
}

// App gathers all necessary dependencies (databases, settings), injects them into the router, and starts the app.
func App(settings *config.Settings, DB *gorm.DB, asyncJobManager *jobs.AsyncJobManager, tagManager brokertags.TagManager) *martini.ClassicMartini {

	m := martini.Classic()

	username := os.Getenv("AUTH_USER")
	password := os.Getenv("AUTH_PASS")

	m.Use(auth.Basic(username, password))
	m.Use(render.Renderer())

	m.Map(DB)
	m.Map(settings)
	m.Map(asyncJobManager)
	m.Map(tagManager)

	path, _ := os.Getwd()
	m.Map(catalog.InitCatalog(path))

	log.Println("Loading Routes")

	// Serve the catalog with services and plans
	m.Get("/v2/catalog", func(r render.Render, c *catalog.Catalog) {
		r.JSON(200, map[string]interface{}{
			"services": c.GetServices(),
		})
	})

	// Create the service instance (cf create-service-instance)
	// This is a PUT per https://github.com/openservicebrokerapi/servicebroker/blob/v2.16/spec.md#provisioning
	m.Put("/v2/service_instances/:id", CreateInstance)

	// Update the service instance
	m.Patch("/v2/service_instances/:id", ModifyInstance)

	// Poll service endpoint to get status of rds or elasticache
	m.Get("/v2/service_instances/:instance_id/last_operation", LastOperation)

	// Bind the service to app (cf bind-service)
	m.Put("/v2/service_instances/:instance_id/service_bindings/:id", BindInstance)

	// Unbind the service from app
	m.Delete("/v2/service_instances/:instance_id/service_bindings/:id", func(p martini.Params, r render.Render) {
		var emptyJSON struct{}
		r.JSON(200, emptyJSON)
	})

	// Delete service instance
	m.Delete("/v2/service_instances/:instance_id", DeleteInstance)

	return m
}
