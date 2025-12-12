package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cloud-gov/aws-broker/common"
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
	SnapshotsBucketName       string
	SnapshotsRepoName         string
	LastSnapshotName          string
	CfApiUrl                  string
	CfApiClientId             string
	CfApiClientSecret         string
	MaxBackupRetention        int64
	MinBackupRetention        int64
	PollAwsMaxRetries         int64
	PollAwsRetryDelaySeconds  int64
	Port                      string
}

// LoadFromEnv loads settings from environment variables
func (s *Settings) LoadFromEnv() error {
	log.Println("Loading settings")
	var err error

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
			return fmt.Errorf("must set environment variable %s", key)
		}
	}

	if os.Getenv("DB_PORT") != "" {
		dbConfig.Port, err = strconv.ParseInt(os.Getenv("DB_PORT"), 10, 64)
		// Just return nothing if we can't interpret the number.
		if err != nil {
			return errors.New("couldn't load port number")
		}
	} else {
		dbConfig.Port = 5432
	}

	s.DbConfig = &dbConfig

	// Load Encryption Key
	if val, ok := os.LookupEnv("ENC_KEY"); ok {
		s.EncryptionKey = val
	} else {
		return errors.New("an encryption key is required. Must specify ENC_KEY environment variable")
	}

	s.DbNamePrefix = os.Getenv("DB_PREFIX")
	if s.DbNamePrefix == "" {
		s.DbNamePrefix = "db"
	}

	s.DbShorthandPrefix = os.Getenv("DB_SHORTHAND_PREFIX")
	if s.DbShorthandPrefix == "" {
		s.DbShorthandPrefix = "db"
	}

	s.Environment = os.Getenv("ENVIRONMENT")

	s.Region = os.Getenv("AWS_DEFAULT_REGION")

	storage := os.Getenv("MAX_ALLOCATED_STORAGE")
	if storage != "" {
		s.MaxAllocatedStorage, err = strconv.ParseInt(storage, 10, 64)
		if err != nil {
			return errors.New("couldn't load max storage")
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

	// set the bucketname created by TF, empty string is ok.
	// broker will check for nil and skip snaphot config
	s.SnapshotsBucketName = os.Getenv("S3_SNAPSHOT_BUCKET")

	// look for snapshot repository name
	s.SnapshotsRepoName = os.Getenv("SNAPSHOTS_REPO_NAME")
	if s.SnapshotsRepoName == "" {
		s.SnapshotsRepoName = "cg-archive"
	}

	s.LastSnapshotName = os.Getenv("LAST_SNAPSHOT_NAME")
	if s.LastSnapshotName == "" {
		s.LastSnapshotName = "cg-last-snapshot"
	}

	if val, ok := os.LookupEnv("MAX_BACKUP_RETENTION"); ok {
		s.MaxBackupRetention, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
	}

	if s.MaxBackupRetention == 0 {
		s.MaxBackupRetention = 35
	}

	if val, ok := os.LookupEnv("MIN_BACKUP_RETENTION"); ok {
		s.MinBackupRetention, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
	}

	if s.MinBackupRetention == 0 {
		s.MinBackupRetention = 14
	}

	if cfApiUrl, ok := os.LookupEnv("CF_API_URL"); ok {
		s.CfApiUrl = cfApiUrl
	} else {
		return errors.New("CF_API_URL environment variable is required")
	}

	if cfApiClient, ok := os.LookupEnv("CF_API_CLIENT_ID"); ok {
		s.CfApiClientId = cfApiClient
	} else {
		return errors.New("CF_API_CLIENT_ID environment variable is required")
	}

	if cfApiClientSecret, ok := os.LookupEnv("CF_API_CLIENT_SECRET"); ok {
		s.CfApiClientSecret = cfApiClientSecret
	} else {
		return errors.New("CF_API_CLIENT_SECRET environment variable is required")
	}

	if val, ok := os.LookupEnv("POLL_AWS_MAX_RETRIES"); ok {
		s.PollAwsMaxRetries, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
	}

	if s.PollAwsMaxRetries == 0 {
		s.PollAwsMaxRetries = 60
	}

	if val, ok := os.LookupEnv("POLL_AWS_RETRY_DELAY_SECONDS"); ok {
		s.PollAwsRetryDelaySeconds, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
	}

	if s.PollAwsRetryDelaySeconds == 0 {
		s.PollAwsRetryDelaySeconds = 60
	}

	if val, ok := os.LookupEnv("PORT"); ok {
		s.Port = val
	}

	if s.Port == "" {
		s.Port = "3000"
	}

	return nil
}
