package main

import (
	"github.com/18F/aws-broker/config"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/db"
	"github.com/18F/aws-broker/services/rds"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func main() {
	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		log.Println("There was an error loading settings")
		log.Println(err)
		return
	}

	// Get the models to migrate.
	var models []interface{}
	models = append(models, new(base.Instance))
	models = append(models, new(rds.Instance))

	DB, err := db.InternalDBInit(settings.DbConfig, models)
	if err != nil {
		log.Println("There was an error with the DB. Error: " + err.Error())
		return
	}

	// Load the catalog data
	path, _ := os.Getwd()
	catalogFile := filepath.Join(path, "catalog.yml")
	catalogData, err := ioutil.ReadFile(catalogFile)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	secretsFile := filepath.Join(path, "secrets.yml")
	secretsData, err := ioutil.ReadFile(secretsFile)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	// Try to connect and create the app.
	if r := App(&settings, DB, catalogData, secretsData); r != nil {
		log.Println("Starting app...")
		r.Run()
	} else {
		log.Println("Unable to setup application. Exiting...")
	}
}

// App gathers all necessary dependencies (databases, settings), injects them into the router, and starts the app.
func App(settings *config.Settings, DB *gorm.DB, catalogData []byte, secretsData []byte) *gin.Engine {
	c := catalog.InitCatalog(catalogData, secretsData)

	r := gin.Default()

	username := os.Getenv("AUTH_USER")
	password := os.Getenv("AUTH_PASS")

	// Group using gin.BasicAuth() middleware
	authorized := r.Group("/", gin.BasicAuth(gin.Accounts{
		username: password,
	}))

	log.Println("Loading Routes")

	InitAPI(authorized, DB, settings, c)

	log.Println("Loaded Routes")

	return r
}
