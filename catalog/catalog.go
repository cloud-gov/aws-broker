package catalog

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

	"errors"

	"gopkg.in/go-playground/validator.v8"
	"gopkg.in/yaml.v2"
)

/*
type Catalog struct {
	Services  []Service `json:"services,omitempty"`
	secrets   Secrets   `yaml:"-" json:"-"`
	resources Resources `yaml:"-" json:"-"`
}
*/

type Catalog struct {
	RDSService RDSService `yaml:"rds" json:"-"`
	SQSService SQSService `yaml:"sqs" json:"-"`
	secrets    Secrets    `yaml:"-" json:"-"`
	resources  Resources  `yaml:"-" json:"-"`
}

type Service struct {
	ID              string          `yaml:"id"`
	Name            string          `yaml:"name"`
	Description     string          `yaml:"description"`
	Bindable        bool            `yaml:"bindable,omitempty"`
	Tags            []string        `yaml:"tags,omitempty"`
	Metadata        ServiceMetadata `yaml:"metadata,omitempty"`
	Requires        []string        `yaml:"requires,omitempty"`
	PlanUpdateable  bool            `yaml:"plan_updateable"`
	DashboardClient DashboardClient `yaml:"dashboard_client,omitempty"`
}

type RDSService struct {
	Service `yaml:",inline"`
	Plans   []RDSPlan `yaml:"plans,omitempty"`
}

type SQSService struct {
	Service `yaml:",inline"`
	Plans   []SQSPlan `yaml:"plans,omitempty"`
}

type ServiceMetadata struct {
	DisplayName         string `yaml:"displayName,omitempty"`
	ImageURL            string `yaml:"imageUrl,omitempty"`
	LongDescription     string `yaml:"longDescription,omitempty"`
	ProviderDisplayName string `yaml:"providerDisplayName,omitempty"`
	DocumentationURL    string `yaml:"documentationUrl,omitempty"`
	SupportURL          string `yaml:"supportUrl,omitempty"`
}

type ServicePlan struct {
	ID          string              `yaml:"id"`
	Name        string              `yaml:"name"`
	Description string              `yaml:"description"`
	Metadata    ServicePlanMetadata `yaml:"metadata,omitempty"`
	Free        bool                `yaml:"free"`
}

type ServicePlanMetadata struct {
	Bullets     []string `yaml:"bullets,omitempty"`
	Costs       []Cost   `yaml:"costs,omitempty"`
	DisplayName string   `yaml:"displayName,omitempty"`
}

type RDSPlan struct {
	ServicePlan `yaml:",inline"`
	Properties  RDSProperties `yaml:",inline"`
}

type SQSPlan struct {
	ServicePlan `yaml:",inline"`
	Properties  SQSProperties `yaml:",inline"`
}

type DashboardClient struct {
	ID          string `yaml:"id,omitempty"`
	Secret      string `yaml:"secret,omitempty"`
	RedirectURI string `yaml:"redirect_uri,omitempty"`
}

type Cost struct {
	Amount map[string]interface{} `yaml:"amount,omitempty"`
	Unit   string                 `yaml:"unit,omitempty"`
}

type RDSProperties struct {
	Adapter       string            `yaml:"adapter" json:"-" validate:"required"`
	InstanceClass string            `yaml:"instanceClass" json:"-"`
	DbType        string            `yaml:"dbType" json:"-" validate:"required"`
	Tags          map[string]string `yaml:"tags" json:"-" validate:"required"`
	SubnetGroup   string            `yaml:"subnetGroup" json:"-" validate:"required"`
	SecurityGroup string            `yaml:"securityGroup" json:"-" validate:"required"`
}

type SQSProperties struct {
	DelaySeconds                  string `yaml:"delay_seconds,omitempty"`
	MaximumMessageSize            string `yaml:"maximum_message_size,omitempty"`
	MessageRetentionPeriod        string `yaml:"message_retention_period,omitempty"`
	Policy                        string `yaml:"policy,omitempty"`
	ReceiveMessageWaitTimeSeconds string `yaml:"receive_message_wait_time_seconds,omitempty"`
	VisibilityTimeout             string `yaml:"visibility_timeout,omitempty"`
}

// ServiceMetadata contains the service metadata fields listed in the Cloud Foundry docs:
// http://docs.cloudfoundry.org/services/catalog-metadata.html#services-metadata-fields
/*
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
*/

func (c Catalog) Validate() error {
	/*for _, service := range c.Services {
		if err := service.Validate(); err != nil {
			return fmt.Errorf("Validating Services configuration: %s", err)
		}
	}
	*/

	return nil
}

/*func (c Catalog) FindService(serviceID string) (service Service, found bool) {
	for _, service := range c.Services {
		if service.ID == serviceID {
			return service, true
		}
	}

	return service, false
}*/

/*func (c Catalog) FindServicePlan(planID string) (plan ServicePlan, found bool) {
	for _, service := range c.Services {
		for _, plan := range service.Plans {
			if plan.ID == planID {
				return plan, true
			}
		}
	}

	return plan, false
}*/

/*func (s Service) Validate() error {
	if s.ID == "" {
		return fmt.Errorf("Must provide a non-empty ID (%+v)", s)
	}

	if s.Name == "" {
		return fmt.Errorf("Must provide a non-empty Name (%+v)", s)
	}

	if s.Description == "" {
		return fmt.Errorf("Must provide a non-empty Description (%+v)", s)
	}

	for _, servicePlan := range s.Plans {
		if err := servicePlan.Validate(); err != nil {
			return fmt.Errorf("Validating Plans configuration: %s", err)
		}
	}

	return nil
}

func (sp ServicePlan) Validate() error {
	if sp.ID == "" {
		return fmt.Errorf("Must provide a non-empty ID (%+v)", sp)
	}

	if sp.Name == "" {
		return fmt.Errorf("Must provide a non-empty Name (%+v)", sp)
	}

	if sp.Description == "" {
		return fmt.Errorf("Must provide a non-empty Description (%+v)", sp)
	}

	if err := sp.SQSProperties.Validate(); err != nil {
		return fmt.Errorf("Validating SQS Properties configuration: %s", err)
	}

	return nil
}*/

func (sq SQSProperties) Validate() error {

	return nil
}

var (
	// ErrNoServiceFound represents the error to return when the service could not be found by its ID.
	ErrNoServiceFound = errors.New("No service found for given service id.")
	// ErrNoPlanFound represents the error to return when the plan could not be found by its ID.
	ErrNoPlanFound = errors.New("No plan found for given plan id.")
)

// RDSService describes the RDS Service. It contains the basic Service details as well as a list of RDS Plans
/*
type RDSService struct {
	Service `yaml:",inline" validate:"required"`
	Plans   []RDSPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}
*/

// FetchPlan will look for a specific Plan based on the plan ID.
/*func (s Service) FetchPlan(planID string) (ServicePlan, response.Response) {
	for _, plan := range s.Plans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return ServicePlan{}, response.NewErrorResponse(http.StatusBadRequest, ErrNoPlanFound.Error())
}
*/
// RDSPlan inherits from a Plan and adds fields specific to AWS.
// these fields are read from the catalog.yaml file, but are not rendered
// in the catalog API endpoint.
/*
type RDSPlan struct {
	Plan          `yaml:",inline" validate:"required"`
	Adapter       string            `yaml:"adapter" json:"-" validate:"required"`
	InstanceClass string            `yaml:"instanceClass" json:"-"`
	DbType        string            `yaml:"dbType" json:"-" validate:"required"`
	Tags          map[string]string `yaml:"tags" json:"-" validate:"required"`
	SubnetGroup   string            `yaml:"subnetGroup" json:"-" validate:"required"`
	SecurityGroup string            `yaml:"securityGroup" json:"-" validate:"required"`
}
*/

// Catalog struct holds a collections of services
/*
type Catalog struct {
	// Instances of Services
	RdsService RDSService `yaml:"rds" json:"-"`

	// All helper structs to be unexported
	secrets   Secrets   `yaml:"-" json:"-"`
	resources Resources `yaml:"-" json:"-"`
}
*/

// Resources contains all the secrets to be used for the catalog.
type Resources struct {
	RdsSettings *RDSSettings
}

// Service struct contains data for the Cloud Foundry service
// http://docs.cloudfoundry.org/services/api.html
/*
type Service struct {
	ID          string          `yaml:"id" json:"id" validate:"required"`
	Name        string          `yaml:"name" json:"name" validate:"required"`
	Description string          `yaml:"description" json:"description" validate:"required"`
	Bindable    bool            `yaml:"bindable" json:"bindable" validate:"required"`
	Tags        []string        `yaml:"tags" json:"tags" validate:"required"`
	Metadata    ServiceMetadata `yaml:"metadata" json:"metadata" validate:"required"`
}
*/

// GetServices returns the list of all the Services. In order to do this, it uses reflection to look for all the
// exported values of the catalog.
func (c *Catalog) GetServices() string {
	/*catalogStruct := reflect.ValueOf(*c)
	numOfFields := catalogStruct.NumField()
	var services []interface{}
	for i := 0; i < numOfFields; i++ {
		structField := catalogStruct.Type().Field(i)
		// Only add the exported values
		if structField.PkgPath == "" {
			services = append(services, catalogStruct.Field(i).Interface())
		}
	}
	return services*/
	sqsService, err := json.Marshal(c.SQSService)
	if err != nil {
		log.Println(err)
	}
	rdsService, err := json.Marshal(c.RDSService)
	if err != nil {
		log.Println(err)
	}
	services := fmt.Sprintf("{ \"services\": [%v, %v] }", string(sqsService), string(rdsService))
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
	catalogFile := filepath.Join(path, "catalog.yaml")
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
