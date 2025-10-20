package catalog

import (
	"io/ioutil"
	"log"
	"path/filepath"

	"github.com/cloud-gov/aws-broker/common"
	"gopkg.in/go-playground/validator.v8"
	"gopkg.in/yaml.v3"
)

// Secrets contains all the secrets for all the services.
type Secrets struct {
	RdsSecret   RDSSecret   `yaml:"rds" validate:"required,dive,required"`
	RedisSecret RedisSecret `yaml:"redis" validate:"required,dive,required"`
}

// RDSSecret is a wrapper for all the RDS Secrets.
// Only contains RDS database secrets as of now.
type RDSSecret struct {
	ServiceID    string        `yaml:"service_id" validate:"required"`
	RDSDBSecrets []RDSDBSecret `yaml:"plans" validate:"required,dive,required"`
}

// RDSDBSecret contains the config to connect to a database and the corresponding plan id.
type RDSDBSecret struct {
	common.DBConfig `yaml:",inline" validate:"required,dive,required"`
	PlanID          string `yaml:"plan_id" validate:"required"`
}

// RedisSecret is a wrapper for all the Redis Secrets.
// Only contains RDS database secrets as of now.
type RedisSecret struct {
	ServiceID      string          `yaml:"service_id" validate:"required"`
	RedisDBSecrets []RedisDBSecret `yaml:"plans" validate:"required,dive,required"`
}

// RedisDBSecret contains the config to connect to a database and the corresponding plan id.
type RedisDBSecret struct {
	common.DBConfig `yaml:",inline" validate:"required,dive,required"`
	PlanID          string `yaml:"plan_id" validate:"required"`
}

// ElasticsearchSecret is a wrapper for all the Elasticsearch Secrets.
// Only contains RDS database secrets as of now.
type ElasticsearchSecret struct {
	ServiceID      string          `yaml:"service_id" validate:"required"`
	RedisDBSecrets []RedisDBSecret `yaml:"plans" validate:"required,dive,required"`
}

// ElasticsearchDBSecret contains the config to connect to a database and the corresponding plan id.
type ElasticsearchDBSecret struct {
	common.DBConfig `yaml:",inline" validate:"required,dive,required"`
	PlanID          string `yaml:"plan_id" validate:"required"`
}

// InitSecrets initializes the secrets struct based on the yaml file.
func InitSecrets(path string) *Secrets {
	var secrets Secrets
	secretsFile := filepath.Join(path, "secrets.yml")
	data, err := ioutil.ReadFile(secretsFile)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = yaml.Unmarshal(data, &secrets)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	config := &validator.Config{TagName: "validate"}

	validate := validator.New(config)
	validateErr := validate.Struct(secrets)
	if validateErr != nil {
		log.Println(validateErr)
		return nil
	}
	return &secrets
}
