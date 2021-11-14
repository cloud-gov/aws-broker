package db

import (
	"log"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/common"
	"github.com/18F/aws-broker/services/elasticsearch"
	"github.com/18F/aws-broker/services/rds"
	"github.com/18F/aws-broker/services/redis"
	"github.com/jinzhu/gorm"
)

const maxDbConnections = 10

// InternalDBInit initializes the internal database connection that the service broker will use.
// In addition to calling DBInit(), it also makes sure that the tables are setup for Instance and DBConfig structs.
func InternalDBInit(dbConfig *common.DBConfig) (*gorm.DB, error) {
	db, err := common.DBInit(dbConfig)
	if err != nil {
		return nil, err
	}
	db.DB().SetMaxOpenConns(maxDbConnections)
	log.Println("Migrating")
	// db.LogMode(true)
	// Automigrate!
	db.AutoMigrate(&rds.RDSInstance{}, &redis.RedisInstance{}, &elasticsearch.ElasticsearchInstance{}, &base.Instance{}) // Add all your models here to help setup the database tables
	log.Println("Migrated")
	return db, err
}
