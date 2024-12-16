package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	brokertags "github.com/cloud-gov/go-broker-tags"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/db"
	"github.com/18F/aws-broker/services/rds"
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

		rows, err := db.Model(&rds.RDSInstance{}).Rows()
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var rdsInstance rds.RDSInstance
			// ScanRows scans a row into a struct
			db.ScanRows(rows, &rdsInstance)

			instanceInfo, err := rdsClient.DescribeDBInstances(&awsRds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(rdsInstance.Database),
			})

			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok {
					if awsErr.Code() == awsRds.ErrCodeDBInstanceNotFoundFault {
						log.Printf("Could not find database %s, continuing", rdsInstance.Database)
						continue
					} else {
						log.Fatalf("Could not find database instance: %s", err)
					}
				} else {
					log.Fatalf("Could not find database instance: %s", err)
				}
			}

			tagsResponse, err := rdsClient.ListTagsForResource(&awsRds.ListTagsForResourceInput{
				ResourceName: instanceInfo.DBInstances[0].DBInstanceArn,
			})
			if err != nil {
				log.Fatalf("error getting tags for database %s: %s", rdsInstance.Database, err)
			}

			log.Printf("found database %s with tags %+v", rdsInstance.Database, tagsResponse.TagList)

			plan, _ := c.RdsService.FetchPlan(rdsInstance.PlanID)
			if plan.Name == "" {
				log.Fatalf("error getting plan %s for database %s", rdsInstance.PlanID, rdsInstance.Database)
			}

			newTags, err := tagManager.GenerateTags(
				brokertags.Create,
				c.RdsService.Name,
				plan.Name,
				brokertags.ResourceGUIDs{
					InstanceGUID:     rdsInstance.Uuid,
					SpaceGUID:        rdsInstance.SpaceGUID,
					OrganizationGUID: rdsInstance.OrganizationGUID,
				},
				true,
			)
			if err != nil {
				log.Fatalf("error generating new tags for database %s: %s", rdsInstance.Database, err)
			}

			log.Printf("generated new tags %+v for database %s", newTags, rdsInstance.Database)
		}
	}
}
