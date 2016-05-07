package env

import (
	"errors"
	"github.com/18F/aws-broker/common/db"
	"log"
	"os"
	"strconv"
)

// SystemEnv stores env settings used to run the application
type SystemEnv struct {
	EncryptionKey string
	DbConfig      db.Config
}

// LoadFromEnv loads settings from environment variables
func (s *SystemEnv) LoadFromEnv() error {
	log.Println("Loading settings")

	// Load DB Settings
	dbConfig := db.Config{}
	dbConfig.DbType = os.Getenv("DB_TYPE")
	dbConfig.URL = os.Getenv("DB_URL")
	dbConfig.Username = os.Getenv("DB_USER")
	dbConfig.Password = os.Getenv("DB_PASS")
	dbConfig.DbName = os.Getenv("DB_NAME")
	if dbConfig.Sslmode = os.Getenv("DB_SSLMODE"); dbConfig.Sslmode == "" {
		dbConfig.Sslmode = "require"
	}

	var err error
	dbConfig.Port, err = strconv.ParseInt(os.Getenv("DB_PORT"), 10, 64)
	// Just return nothing if we can't interpret the number.
	if err != nil {
		return errors.New("Couldn't load port number")
	}

	s.DbConfig = dbConfig

	// Load Encryption Key
	s.EncryptionKey = os.Getenv("ENC_KEY")
	if s.EncryptionKey == "" {
		return errors.New("An encryption key is required")
	}

	// TODO: Make sure all the values are valid
	return nil
}
