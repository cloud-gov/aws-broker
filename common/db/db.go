package db

import (
	"github.com/jinzhu/gorm"
	"log"
)

// InternalDBInit initializes the internal database connection that the service broker will use.
// In addition to calling DBInit(), it also makes sure that the tables are setup for Instance and DBConfig structs.
func InternalDBInit(dbConfig Config, models []interface{}) (*gorm.DB, error) {
	db, err := Init(dbConfig)
	if err == nil {
		db.DB().SetMaxOpenConns(10)
		log.Println("Migrating")
		db.AutoMigrate(models...)
		log.Println("Migrated")
	}
	return db, err
}
