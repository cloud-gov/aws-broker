package catalog

import (
	"log"
	"os"
	"path/filepath"

	"errors"
	"reflect"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"gopkg.in/go-playground/validator.v8"
	"gopkg.in/yaml.v3"
)

var (
	// ErrNoServiceFound represents the error to return when the service could not be found by its ID.
	ErrNoServiceFound = errors.New("no service found for given service id")
	// ErrNoPlanFound represents the error to return when the plan could not be found by its ID.
	ErrNoPlanFound = errors.New("no plan found for given plan id")
)

type Service struct {
	ID                   string                         `json:"id"`
	Name                 string                         `json:"name"`
	Description          string                         `json:"description"`
	Bindable             bool                           `json:"bindable"`
	InstancesRetrievable bool                           `json:"instances_retrievable,omitempty"`
	BindingsRetrievable  bool                           `json:"bindings_retrievable,omitempty"`
	Tags                 []string                       `json:"tags,omitempty"`
	PlanUpdatable        bool                           `json:"plan_updateable"`
	Requires             []domain.RequiredPermission    `json:"requires,omitempty"`
	Metadata             *domain.ServiceMetadata        `json:"metadata,omitempty"`
	DashboardClient      *domain.ServiceDashboardClient `json:"dashboard_client,omitempty"`
	AllowContextUpdates  bool                           `json:"allow_context_updates,omitempty"`
}

// Catalog struct holds a collections of services
type Catalog struct {
	// Instances of Services
	RdsService           *RDSService          `yaml:"rds" json:"-"`
	RedisService         RedisService         `yaml:"redis" json:"-"`
	ElasticsearchService ElasticsearchService `yaml:"elasticsearch" json:"-"`

	// All helper structs to be unexported
	resources Resources `yaml:"-" json:"-"`
}

// Resources contains all the secrets to be used for the catalog.
type Resources struct {
	RdsSettings *RDSSettings
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
			if service, ok := catalogStruct.Field(i).Interface().(domain.Service); ok {
				services = append(services, service)
			}
		}
	}
	return services
}

func (c *Catalog) GetServices2() []domain.Service {
	return []domain.Service{c.RdsService.ToBrokerAPIService(), c.ElasticsearchService.ToBrokerAPIService(), c.RedisService.ToBrokerAPIService()}
}

// GetResources returns the resources wrapper for all the resources generated from the secrets.
func (c *Catalog) GetResources() Resources {
	return c.resources
}

// InitCatalog initializes a Catalog struct that contains services and plans
// defined in the catalog.yaml configuration file and returns a pointer to that catalog
func InitCatalog(path string) *Catalog {
	var catalog Catalog
	catalogFile := filepath.Join(path, "catalog.yml")
	data, err := os.ReadFile(catalogFile)
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
		return errors.New("unable to load secrets")
	}

	// Loading resources.
	rdsSettings, err := InitRDSSettings(secrets)
	if err != nil {
		return errors.New("unable to load rds settings")
	}
	c.resources.RdsSettings = rdsSettings
	return nil
}
