package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/opensearchservice"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	brokertags "github.com/cloud-gov/go-broker-tags"

	tasksElasticache "github.com/cloud-gov/aws-broker/cmd/tasks/elasticache"
	tasksOpensearch "github.com/cloud-gov/aws-broker/cmd/tasks/opensearch"
	"github.com/cloud-gov/aws-broker/cmd/tasks/rds"
	tasksRds "github.com/cloud-gov/aws-broker/cmd/tasks/rds"

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
	actionPtr := flag.String("action", "", "Action to take. Accepted options: 'reconcile-tags', 'reconcile-log-groups'")
	flag.Var(&servicesToTag, "service", "Specify AWS service whose instances should have tags updated. Accepted options: 'rds', 'elasticache', 'elasticsearch', 'opensearch'")
	flag.Parse()

	if *actionPtr == "" {
		log.Fatal("--action flag is required")
	}

	if len(servicesToTag) == 0 {
		return errors.New("--service argument is required. Specify --service multiple times to update tags for multiple services")
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

	if *actionPtr == "reconcile-tags" {
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

		logsClient := cloudwatchlogs.New(sess)

		if slices.Contains(servicesToTag, "rds") {
			rdsClient := awsRds.New(sess)
			err := tasksRds.ReconcileRDSResourceTags(c, db, rdsClient, logsClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(servicesToTag, "elasticache") {
			elasticacheClient := elasticache.New(sess)
			err := tasksElasticache.ReconcileElasticacheResourceTags(c, db, elasticacheClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(servicesToTag, "elasticsearch") || slices.Contains(servicesToTag, "opensearch") {
			opensearchClient := opensearchservice.New(sess)
			err := tasksOpensearch.ReconcileOpensearchResourceTags(c, db, opensearchClient, tagManager)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "reconcile-log-groups" {
		logsClient := cloudwatchlogs.New(sess)

		if slices.Contains(servicesToTag, "rds") {
			rdsClient := awsRds.New(sess)
			err := rds.ReconcileRDSCloudwatchLogGroups(logsClient, rdsClient, settings.DbNamePrefix, db)
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
