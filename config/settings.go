package config

import (
	"errors"
	"fmt"
	"github.com/18F/aws-broker/common"
	"log"
	"os"
	"strconv"
)

// Settings stores settings used to run the application
type Settings struct {
	EncryptionKey             string
	DbNamePrefix              string
	DbShorthandPrefix         string
	MaxAllocatedStorage       int64
	DbConfig                  *common.DBConfig
	Environment               string
	Region                    string
	PubliclyAccessibleFeature bool
	EnableFunctionsFeature    bool
}

// LoadFromEnv loads settings from environment variables
func (s *Settings) LoadFromEnv() error {
	log.Println("Loading settings")

	// Load DB Settings
	dbConfig := common.DBConfig{}
	dbConfig.DbType = os.Getenv("DB_TYPE")
	dbConfig.URL = os.Getenv("DB_URL")
	dbConfig.Username = os.Getenv("DB_USER")
	dbConfig.Password = os.Getenv("DB_PASS")
	dbConfig.DbName = os.Getenv("DB_NAME")
	if dbConfig.Sslmode = os.Getenv("DB_SSLMODE"); dbConfig.Sslmode == "" {
		dbConfig.Sslmode = "require"
	}

	// Ensure AWS credentials exist in environment
	for _, key := range []string{"AWS_DEFAULT_REGION"} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("Must set environment variable %s", key)
		}
	}

	if os.Getenv("DB_PORT") != "" {
		var err error
		dbConfig.Port, err = strconv.ParseInt(os.Getenv("DB_PORT"), 10, 64)
		// Just return nothing if we can't interpret the number.
		if err != nil {
			return errors.New("Couldn't load port number")
		}
	} else {
		dbConfig.Port = 5432
	}

	s.DbConfig = &dbConfig

	// Load Encryption Key
	s.EncryptionKey = os.Getenv("ENC_KEY")
	if s.EncryptionKey == "" {
		return errors.New("An encryption key is required")
	}

	s.DbNamePrefix = os.Getenv("DB_PREFIX")
	if s.DbNamePrefix == "" {
		s.DbNamePrefix = "db"
	}

	s.DbShorthandPrefix = os.Getenv("DB_SHORTHAND_PREFIX")
	if s.DbShorthandPrefix == "" {
		s.DbShorthandPrefix = "db"
	}

	// Set env to production
	s.Environment = "production"

	s.Region = os.Getenv("AWS_DEFAULT_REGION")

	storage := os.Getenv("MAX_ALLOCATED_STORAGE")
	if storage != "" {
		var err error
		s.MaxAllocatedStorage, err = strconv.ParseInt(storage, 10, 64)
		if err != nil {
			return errors.New("Couldn't load max storage")
		}
	} else {
		s.MaxAllocatedStorage = 1024
	}

	// Feature flag to allow RDS to be publicly available (needed for testing)
	if _, ok := os.LookupEnv("PUBLICLY_ACCESSIBLE"); ok {
		s.PubliclyAccessibleFeature = true
	} else {
		s.PubliclyAccessibleFeature = false
	}

	// Feature flag to allow mysql to be provisioned with log_bin_trust_function_creators=1
	if _, ok := os.LookupEnv("ENABLE_FUNCTIONS"); ok {
		s.EnableFunctionsFeature = true
	} else {
		s.EnableFunctionsFeature = false
	}

	return nil
}
