package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/sts"

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

	stsClient := sts.New(sess)
	info, err := stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		log.Fatalf("Could not get caller identity: %s", err)
	}

	if slices.Contains(servicesToTag, "rds") {
		rows, err := db.Model(&rds.RDSInstance{}).Rows()
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var rdsInstance rds.RDSInstance
			// ScanRows scans a row into a struct
			db.ScanRows(rows, &rdsInstance)

			rdsClient := awsRds.New(sess)
			tagsResponse, err := rdsClient.ListTagsForResource(&awsRds.ListTagsForResourceInput{
				ResourceName: aws.String(fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", settings.Region, *info.Account, rdsInstance.Database)),
			})
			if err != nil {
				log.Fatalf("error getting tags for database %s: %s", rdsInstance.Database, err)
			}

			log.Printf("found database %s with tags %s", rdsInstance.Database, tagsResponse.TagList)
		}
	}
}
