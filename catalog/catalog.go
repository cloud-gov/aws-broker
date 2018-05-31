package catalog

import (
	"io/ioutil"
	"log"
	"path/filepath"

	"errors"
	"net/http"
	"reflect"

	"github.com/18F/aws-broker/helpers/response"
	"gopkg.in/go-playground/validator.v8"
	"gopkg.in/yaml.v2"
)

// ServiceMetadata contains the service metadata fields listed in the Cloud Foundry docs:
// http://docs.cloudfoundry.org/services/catalog-metadata.html#services-metadata-fields
type ServiceMetadata struct {
	DisplayName         string `yaml:"displayName" json:"displayName"`
	ImageURL            string `yaml:"imageUrl" json:"imageUrl"`
	LongDescription     string `yaml:"longDescription" json:"longDescription"`
	ProviderDisplayName string `yaml:"providerDisplayName" json:"providerDisplayName"`
	DocumentationURL    string `yaml:"documentationUrl" json:"documentationUrl"`
	SupportURL          string `yaml:"supportUrl" json:"supportUrl"`
}

// PlanCost contains an array-of-objects that describes the costs of a service,
// in what currency, and the unit of measure.
type PlanCost struct {
	Amount map[string]float64 `yaml:"amount" json:"amount" validate:"required"`
	Unit   string             `yaml:"unit" json:"unit" validate:"required"`
}

// PlanMetadata contains the plan metadata fields listed in the Cloud Foundry docs:
// http://docs.cloudfoundry.org/services/catalog-metadata.html#plan-metadata-fields
type PlanMetadata struct {
	Bullets     []string   `yaml:"bullets" json:"bullets"`
	Costs       []PlanCost `yaml:"costs" json:"costs"`
	DisplayName string     `yaml:"displayName" json:"displayName"`
}

// Plan is a generic struct for a Cloud Foundry service plan
// http://docs.cloudfoundry.org/services/api.html
type Plan struct {
	ID          string       `yaml:"id" json:"id" validate:"required"`
	Name        string       `yaml:"name" json:"name" validate:"required"`
	Description string       `yaml:"description" json:"description" validate:"required"`
	Metadata    PlanMetadata `yaml:"metadata" json:"metadata" validate:"required"`
	Free        bool         `yaml:"free" json:"free"`
}

var (
	// ErrNoServiceFound represents the error to return when the service could not be found by its ID.
	ErrNoServiceFound = errors.New("No service found for given service id.")
	// ErrNoPlanFound represents the error to return when the plan could not be found by its ID.
	ErrNoPlanFound = errors.New("No plan found for given plan id.")
)

// RDSService describes the RDS Service. It contains the basic Service details as well as a list of RDS Plans
type RDSService struct {
	Service `yaml:",inline" validate:"required"`
	Plans   []RDSPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific RDS Plan based on the plan ID.
func (s RDSService) FetchPlan(planID string) (RDSPlan, response.Response) {
	for _, plan := range s.Plans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return RDSPlan{}, response.NewErrorResponse(http.StatusBadRequest, ErrNoPlanFound.Error())
}

// RDSPlan inherits from a Plan and adds fields specific to AWS.
// these fields are read from the catalog.yaml file, but are not rendered
// in the catalog API endpoint.
type RDSPlan struct {
	Plan                  `yaml:",inline" validate:"required"`
	Adapter               string            `yaml:"adapter" json:"-" validate:"required"`
	InstanceClass         string            `yaml:"instanceClass" json:"-"`
	DbType                string            `yaml:"dbType" json:"-" validate:"required"`
	DbVersion             string            `yaml:"dbVersion" json:"-"`
	LicenseModel          string            `yaml:"licenseModel" json:"-"`
	Tags                  map[string]string `yaml:"tags" json:"-" validate:"required"`
	Redundant             bool              `yaml:"redundant" json:"-"`
	Encrypted             bool              `yaml:"encrypted" json:"-"`
	StorageType           string            `yaml:"storage_type" json:"-"`
	AllocatedStorage      int64             `yaml:"allocatedStorage" json:"-"`
	BackupRetentionPeriod int64             `yaml:"backup_retention_period" json:"-"" validate:"required"`
	SubnetGroup           string            `yaml:"subnetGroup" json:"-" validate:"required"`
	SecurityGroup         string            `yaml:"securityGroup" json:"-" validate:"required"`
}

// Catalog struct holds a collections of services
type Catalog struct {
	// Instances of Services
	RdsService RDSService `yaml:"rds" json:"-"`

	// All helper structs to be unexported
	secrets   Secrets   `yaml:"-" json:"-"`
	resources Resources `yaml:"-" json:"-"`
}

// Resources contains all the secrets to be used for the catalog.
type Resources struct {
	RdsSettings *RDSSettings
}

// Service struct contains data for the Cloud Foundry service
// http://docs.cloudfoundry.org/services/api.html
type Service struct {
	ID          string          `yaml:"id" json:"id" validate:"required"`
	Name        string          `yaml:"name" json:"name" validate:"required"`
	Description string          `yaml:"description" json:"description" validate:"required"`
	Bindable    bool            `yaml:"bindable" json:"bindable" validate:"required"`
	Tags        []string        `yaml:"tags" json:"tags" validate:"required"`
	Metadata    ServiceMetadata `yaml:"metadata" json:"metadata" validate:"required"`
}

// GetServices returns the list of all the Services. In order to do this, it uses reflection to look for all the
// exported values of the catalog.
func (c *Catalog) GetServices() []interface{} {
	catalogStruct := reflect.ValueOf(*c)
	numOfFields := catalogStruct.NumField()
	var services []interface{}
	for i := 0; i < numOfFields; i++ {
		structField := catalogStruct.Type().Field(i)
		// Only add the exported values
		if structField.PkgPath == "" {
			services = append(services, catalogStruct.Field(i).Interface())
		}
	}
	return services
}

// GetResources returns the resources wrapper for all the resources generated from the secrets. (e.g. Connection to shared dbs)
func (c *Catalog) GetResources() Resources {
	return c.resources
}

// InitCatalog initializes a Catalog struct that contains services and plans
// defined in the catalog.yaml configuration file and returns a pointer to that catalog
func InitCatalog(path string) *Catalog {
	var catalog Catalog
	catalogFile := filepath.Join(path, "catalog.yml")
	data, err := ioutil.ReadFile(catalogFile)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = yaml.Unmarshal(data, &catalog)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	config := &validator.Config{TagName: "validate"}

	validate := validator.New(config)
	validateErr := validate.Struct(catalog)
	if validateErr != nil {
		log.Println(validateErr)
		return nil
	}

	err = catalog.loadServicesResources(path)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	return &catalog
}

func (c *Catalog) loadServicesResources(path string) error {
	// Load secrets
	secrets := InitSecrets(path)
	if secrets == nil {
		return errors.New("Unable to load secrets.")
	}

	// Loading resources.
	rdsSettings, err := InitRDSSettings(secrets)
	if err != nil {
		return errors.New("Unable to load rds settings.")
	}
	c.resources.RdsSettings = rdsSettings
	return nil
}
