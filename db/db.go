package db

import (
	"log"

	async_jobs "github.com/cloud-gov/aws-broker/async_jobs"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	"github.com/cloud-gov/aws-broker/services/rds"
	"github.com/cloud-gov/aws-broker/services/redis"
	"gorm.io/gorm"
)

const maxDbConnections = 10

// InternalDBInit initializes the internal database connection that the service broker will use.
// In addition to calling DBInit(), it also makes sure that the tables are setup for Instance and DBConfig structs.
func InternalDBInit(dbConfig *common.DBConfig) (*gorm.DB, error) {
	db, err := common.DBInit(dbConfig)
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(maxDbConnections)
	log.Println("Migrating")
	// Automigrate!
	db.AutoMigrate(&rds.RDSInstance{}, &redis.RedisInstance{}, &elasticsearch.ElasticsearchInstance{}, &base.Instance{}, &async_jobs.AsyncJobMsg{}) // Add all your models here to help setup the database tables
	log.Println("Migrated")
	return db, err
}
