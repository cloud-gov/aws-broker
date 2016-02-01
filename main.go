package main

import (
	"github.com/18F/aws-broker/config"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/db"
	"github.com/gin-gonic/gin"
	"log"
	"os"
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
	if r := App(&settings, DB); r != nil {
		log.Println("Starting app...")
		r.Run()
	} else {
		log.Println("Unable to setup application. Exiting...")
	}
}

// App gathers all necessary dependencies (databases, settings), injects them into the router, and starts the app.
func App(settings *config.Settings, DB *gorm.DB) *gin.Engine {

	r := gin.Default()

	username := os.Getenv("AUTH_USER")
	password := os.Getenv("AUTH_PASS")

	// Group using gin.BasicAuth() middleware
	authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
		username: password,
	}))

	path, _ := os.Getwd()
	c := catalog.InitCatalog(path)

	log.Println("Loading Routes")

	InitAPI(authorized, DB, settings, c)

	log.Println("Loaded Routes")

	return r
}
