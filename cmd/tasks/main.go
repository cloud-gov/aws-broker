package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	brokertags "github.com/cloud-gov/go-broker-tags"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/db"
	"golang.org/x/exp/slices"
)

type serviceNames []string

// String is an implementation of the flag.Value interface
func (s *serviceNames) String() string {
	return fmt.Sprintf("%v", *s)
}

// Set is an implementation of the flag.Value interface
func (s *serviceNames) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var servicesToTag serviceNames

func main() {
	flag.Var(&servicesToTag, "service", "Name of AWS service to update tags. Accepted options: 'rds'")
	flag.Parse()

	if len(servicesToTag) == 0 {
		log.Fatal("no services specified")
	}

	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		log.Fatalf("There was an error loading settings: %s", err)
		return
	}

	db, err := db.InternalDBInit(settings.DbConfig)
	if err != nil {
		log.Fatalf("There was an error with the DB. Error: %s", err.Error())
		return
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(settings.Region),
	})
	if err != nil {
		log.Fatalf("Could not initialize session: %s", err)
	}

	tagManager, err := brokertags.NewCFTagManager(
		"AWS broker",
		settings.Environment,
		settings.CfApiUrl,
		settings.CfApiClientId,
		settings.CfApiClientSecret,
	)
	if err != nil {
		log.Fatalf("Could not initialize tag manager: %s", err)
	}

	path, _ := os.Getwd()
	c := catalog.InitCatalog(path)

	if slices.Contains(servicesToTag, "rds") {
		rdsClient := awsRds.New(sess)
		fetchAndUpdateRdsInstanceTags(c, db, rdsClient, tagManager)
	}
}
