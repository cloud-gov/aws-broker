package main

import (
	"log"

	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/db"
	"github.com/18F/aws-broker/services/rds"
)

func main() {
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
