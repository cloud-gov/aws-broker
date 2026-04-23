package testutil

import (
	"fmt"
	"os"

	"github.com/cloud-gov/aws-broker/db"

	"gorm.io/gorm"
)

func InitTestDbConfig() (*db.DBConfig, error) {
	var dbConfig db.DBConfig
	if dbConfig.DbType = os.Getenv("DB_TYPE"); dbConfig.DbType == "" {
		dbConfig.DbType = "sqlite3"
	}
	switch dbConfig.DbType {
	case "postgres":
		dbConfig.DbType = "postgres"
		dbConfig.DbName = os.Getenv("POSTGRES_USER")
		dbConfig.Password = os.Getenv("POSTGRES_PASSWORD")
		dbConfig.Sslmode = "disable"
		dbConfig.Port = 5432
		dbConfig.Username = os.Getenv("POSTGRES_USER")
		dbConfig.URL = "localhost"
	case "sqlite3":
		dbConfig.DbType = "sqlite3"
		dbConfig.DbName = ":memory:"
	default:
		return nil, fmt.Errorf("unsupported db type: %s", dbConfig.DbType)
	}
	return &dbConfig, nil
}

func TestDbInit() (*gorm.DB, error) {
	config, err := InitTestDbConfig()
	if err != nil {
		return nil, err
	}
	db, err := db.DBInit(config)
	if err != nil {
		return nil, err
	}
	return db, nil
}
