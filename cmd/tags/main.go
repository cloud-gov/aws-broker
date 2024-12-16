package main

import (
	"flag"
	"fmt"
	"log"

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
	flag.Var(&servicesToTag, "service", "Name of AWS service to update tags (e.g. 'rds')")
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

	if slices.Contains(servicesToTag, "rds") {
		rows, err := db.Model(&rds.RDSInstance{}).Rows()
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var rdsInstance rds.RDSInstance
			// ScanRows scans a row into a struct
			db.ScanRows(rows, &rdsInstance)

			// Perform operations on each user
			log.Printf("found database %s", rdsInstance.Database)
		}
	}
}
