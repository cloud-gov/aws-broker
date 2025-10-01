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
	tasksRds "github.com/cloud-gov/aws-broker/cmd/tasks/rds"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"

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

var services serviceNames

func run() error {
	actionPtr := flag.String("action", "", "Action to take. Accepted options: 'reconcile-tags', 'reconcile-log-groups', 'reconcile-parameter-groups', 'find-orphaned-instances'")
	flag.Var(&services, "service", "Specify AWS service whose instances should have tags updated. Accepted options: 'rds', 'elasticache', 'elasticsearch', 'opensearch'")
	flag.Parse()

	if *actionPtr == "" {
		log.Fatal("--action flag is required")
	}

	if len(services) == 0 {
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

		if slices.Contains(services, "rds") {
			rdsClient := awsRds.New(sess)
			err := tasksRds.ReconcileResourceTagsForAllRDSDatabases(c, db, rdsClient, logsClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(services, "elasticache") {
			elasticacheClient := elasticache.New(sess)
			err := tasksElasticache.ReconcileElasticacheResourceTags(c, db, elasticacheClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(services, "elasticsearch") || slices.Contains(services, "opensearch") {
			opensearchClient := opensearchservice.New(sess)
			err := tasksOpensearch.ReconcileOpensearchResourceTags(c, db, opensearchClient, tagManager)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "reconcile-log-groups" {
		logsClient := cloudwatchlogs.New(sess)

		if slices.Contains(services, "rds") {
			rdsClient := awsRds.New(sess)
			err := tasksRds.ReconcileRDSCloudwatchLogGroups(logsClient, rdsClient, settings.DbNamePrefix, db)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "reconcile-parameter-groups" {
		if slices.Contains(services, "rds") {
			rdsClient := awsRds.New(sess)
			err := tasksRds.ReconcileRDSParameterGroups(rdsClient, db)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "find-orphaned-instances" {
		if slices.Contains(services, "rds") {
			rdsClient := awsRds.New(sess)
			err := tasksRds.FindOrphanedInstances(rdsClient, db, settings.DbNamePrefix, settings.DbConfig.URL)
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
