package catalog

import (
	"github.com/18F/aws-broker/common/db"
	"gopkg.in/go-playground/validator.v8"
	"gopkg.in/yaml.v2"
	"log"
)

// Secrets contains all the secrets for all the services.
type Secrets struct {
	RdsSecret RDSSecret `yaml:"rds" validate:"required,dive,required"`
}

// RDSSecret is a wrapper for all the RDS Secrets.
// Only contains RDS database secrets as of now.
type RDSSecret struct {
	ServiceID    string        `yaml:"service_id" validate:"required"`
	RDSDBSecrets []RDSDBSecret `yaml:"plans" validate:"required,dive,required"`
}

// RDSDBSecret contains the config to connect to a database and the corresponding plan id.
type RDSDBSecret struct {
	db.Config `yaml:",inline" validate:"required,dive,required"`
	PlanID    string `yaml:"plan_id" validate:"required"`
}

// InitSecrets initializes the secrets struct based on the yaml file.
func InitSecrets(data []byte) *Secrets {
	var secrets Secrets
	err := yaml.Unmarshal(data, &secrets)
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
