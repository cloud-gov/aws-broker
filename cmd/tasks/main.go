package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/rds"
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
	actionPtr := flag.String("action", "", "Action to take. Accepted options: 'reconcile-tags', 'reconcile-log-groups', 'reconcile-parameter-groups', 'find-orphaned-instances', 'apply-elasticache-service-updates'")
	flag.Var(&services, "service", "Specify AWS service whose instances should have tags updated. Accepted options: 'rds', 'elasticache', 'elasticsearch', 'opensearch'")
	dryRunPtr := flag.Bool("dry-run", false, "For 'apply-elasticache-service-updates': discover and log what would be applied without calling AWS to apply anything. Use this to preview a run before allowing it to mutate anything (e.g. the first run in a newly-enabled environment).")
	batchSizePtr := flag.Int("batch-size", tasksElasticache.DefaultApplyBatchSize, "For 'apply-elasticache-service-updates': maximum number of replication groups to include in a single apply call, bounding how many simultaneous failovers/reboots a single run can trigger. Clamped to the AWS hard limit of 20.")
	staggerDelayPtr := flag.Duration("stagger-delay", tasksElasticache.DefaultStaggerDelay, "For 'apply-elasticache-service-updates': pause between consecutive apply batches (e.g. '5m'), spreading correlated failovers/reboots out over time instead of triggering them all at once.")
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

	cfg, err := awsConfig.LoadDefaultConfig(
		context.TODO(),
		awsConfig.WithRegion(settings.Region),
	)
	if err != nil {
		return fmt.Errorf("error loading AWS config: %w", err)
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

		logsClient := cloudwatchlogs.NewFromConfig(cfg)

		if slices.Contains(services, "rds") {
			rdsClient := rds.NewFromConfig(cfg)
			err := tasksRds.ReconcileResourceTagsForAllRDSDatabases(c, db, rdsClient, logsClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(services, "elasticache") {
			elasticacheClient := elasticache.NewFromConfig(cfg)
			err := tasksElasticache.ReconcileElasticacheResourceTags(c, db, elasticacheClient, tagManager)
			if err != nil {
				return err
			}
		}
		if slices.Contains(services, "elasticsearch") || slices.Contains(services, "opensearch") {
			opensearchClient := opensearch.NewFromConfig(cfg)
			err := tasksOpensearch.ReconcileOpensearchResourceTags(c, db, opensearchClient, tagManager)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "reconcile-log-groups" {
		logsClient := cloudwatchlogs.NewFromConfig(cfg)

		if slices.Contains(services, "rds") {
			rdsClient := rds.NewFromConfig(cfg)
			err := tasksRds.ReconcileRDSCloudwatchLogGroups(logsClient, rdsClient, settings.DbNamePrefix, db)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "reconcile-parameter-groups" {
		if slices.Contains(services, "rds") {
			rdsClient := rds.NewFromConfig(cfg)
			err := tasksRds.ReconcileRDSParameterGroups(rdsClient, db)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "find-orphaned-instances" {
		if slices.Contains(services, "rds") {
			rdsClient := rds.NewFromConfig(cfg)
			err := tasksRds.FindOrphanedInstances(rdsClient, db, settings.DbNamePrefix, settings.DbConfig.URL)
			if err != nil {
				return err
			}
		}
	}

	if *actionPtr == "apply-elasticache-service-updates" {
		if slices.Contains(services, "elasticache") {
			elasticacheClient := elasticache.NewFromConfig(cfg)
			opts := tasksElasticache.ApplyOptions{
				BatchSize:    *batchSizePtr,
				StaggerDelay: *staggerDelayPtr,
				DryRun:       *dryRunPtr,
				Sleep:        time.Sleep,
			}
			err := tasksElasticache.ApplyElasticacheServiceUpdates(db, elasticacheClient, opts)
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
