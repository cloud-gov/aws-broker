package main

import (
	"github.com/18F/aws-broker/config"
	"github.com/go-martini/martini"
	"github.com/jinzhu/gorm"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"

	"log"
	"os"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/db"
)

func main() {
	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		log.Println("There was an error loading settings")
		log.Println(err)
		return
	}

	DB, err := db.InternalDBInit(settings.DbConfig)
	if err != nil {
		log.Println("There was an error with the DB. Error: " + err.Error())
		return
	}

	// Try to connect and create the app.
	if m := App(&settings, DB); m != nil {
		log.Println("Starting app...")
		m.Run()
	} else {
		log.Println("Unable to setup application. Exiting...")
	}
}

// App gathers all necessary dependencies (databases, settings), injects them into the router, and starts the app.
func App(settings *config.Settings, DB *gorm.DB) *martini.ClassicMartini {

	m := martini.Classic()

	username := os.Getenv("AUTH_USER")
	password := os.Getenv("AUTH_PASS")

	m.Use(auth.Basic(username, password))
	m.Use(render.Renderer())

	m.Map(DB)
	m.Map(settings)

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
	m.Put("/v2/service_instances/:id", CreateInstance)

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
