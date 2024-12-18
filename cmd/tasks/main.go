package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/opensearchservice"
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

func run() error {
	actionPtr := flag.String("action", "", "Action to take. Accepted options: 'update-tags'")
	flag.Var(&servicesToTag, "service", "Specify AWS service whose instances should have tags updated. Accepted options: 'rds', 'elasticache', 'elasticsearch', 'opensearch'")
	flag.Parse()

	if *actionPtr == "" {
		log.Fatal("--action flag is required")
	}

	var settings config.Settings

	// Load settings from environment
	if err := settings.LoadFromEnv(); err != nil {
		return fmt.Errorf("there was an error loading settings: %w", err)
	}

	db, err := db.InternalDBInit(settings.DbConfig)
	if err != nil {
		return fmt.Errorf("there was an error with the DB. Error: %s", err.Error())
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(settings.Region),
	})
	if err != nil {
		return fmt.Errorf("could not initialize session: %s", err)
	}

	if *actionPtr == "update-tags" {
		if len(servicesToTag) == 0 {
			return errors.New("--service argument is required. Specify --service multiple times to update tags for multiple services")
		}

		tagManager, err := brokertags.NewCFTagManager(
			"AWS broker",
			settings.Environment,
			settings.CfApiUrl,
			settings.CfApiClientId,
			settings.CfApiClientSecret,
		)
		if err != nil {
			return fmt.Errorf("could not initialize tag manager: %s", err)
		}

		path, _ := os.Getwd()
		c := catalog.InitCatalog(path)

		if slices.Contains(servicesToTag, "rds") {
			rdsClient := awsRds.New(sess)
			err := reconcileRDSResourceTags(c, db, rdsClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(servicesToTag, "elasticache") {
			elasticacheClient := elasticache.New(sess)
			err := fetchAndUpdateElasticacheInstanceTags(c, db, elasticacheClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(servicesToTag, "elasticsearch") || slices.Contains(servicesToTag, "opensearch") {
			opensearchClient := opensearchservice.New(sess)
			err := fetchAndUpdateOpensearchInstanceTags(c, db, opensearchClient, tagManager)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err.Error())
	}
}
