package catalog

import (
	"log"
	"os"
	"path/filepath"

	"errors"
	"net/http"
	"reflect"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/helpers/response"
	"gopkg.in/go-playground/validator.v8"
	"gopkg.in/yaml.v2"
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

// RDSService describes the RDS Service. It contains the basic Service details as well as a list of RDS Plans
type RDSService struct {
	Service `yaml:",inline" validate:"required"`
	Plans   []RDSPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific RDS Plan based on the plan ID.
func (s RDSService) FetchPlan(planID string) (RDSPlan, error) {
	for _, plan := range s.Plans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return RDSPlan{}, ErrNoPlanFound
}

// RDSPlan inherits from a Plan and adds fields specific to AWS.
// these fields are read from the catalog.yaml file, but are not rendered
// in the catalog API endpoint.
type RDSPlan struct {
	domain.ServicePlan    `yaml:",inline" validate:"required"`
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
	BackupRetentionPeriod int64             `yaml:"backup_retention_period" json:"-" validate:"required"`
	SubnetGroup           string            `yaml:"subnetGroup" json:"-" validate:"required"`
	SecurityGroup         string            `yaml:"securityGroup" json:"-" validate:"required"`
	ApprovedMajorVersions []string          `yaml:"approvedMajorVersions" json:"-"`
	ReadReplica           bool              `yaml:"read_replica" json:"-"`
}

// CheckVersion verifies that a specific version chosen by the user for a new
// RDS instances is valid and supported in the chosen plan.
func (p RDSPlan) CheckVersion(version string) bool {
	// Return true if there are no valid major versions set in the plan; this
	// lets the calls proceed and the AWS API will error out if an invalid
	// version is provided.
	if len(p.ApprovedMajorVersions) == 0 {
		return true
	}

	for _, approvedVersion := range p.ApprovedMajorVersions {
		if version == approvedVersion {
			return true
		}
	}

	return false
}

// RedisService describes the Redis Service. It contains the basic Service details as well as a list of Redis Plans
type RedisService struct {
	Service `yaml:",inline" validate:"required"`
	Plans   []RedisPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific RedisSecret Plan based on the plan ID.
func (s RedisService) FetchPlan(planID string) (RedisPlan, response.Response) {
	for _, plan := range s.Plans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return RedisPlan{}, response.NewErrorResponse(http.StatusBadRequest, ErrNoPlanFound.Error())
}

// RedisPlan inherits from a plan and adds fields needed for AWS Redis.
type RedisPlan struct {
	domain.ServicePlan         `yaml:",inline" validate:"required"`
	Tags                       map[string]string `yaml:"tags" json:"-" validate:"required"`
	EngineVersion              string            `yaml:"engineVersion" json:"-"`
	SubnetGroup                string            `yaml:"subnetGroup" json:"-" validate:"required"`
	SecurityGroup              string            `yaml:"securityGroup" json:"-" validate:"required"`
	CacheNodeType              string            `yaml:"nodeType" json:"-" validate:"required"`
	NumCacheClusters           int               `yaml:"numberCluster" json:"-" validate:"required"`
	PreferredMaintenanceWindow string            `yaml:"preferredMaintenanceWindow" json:"-" validate:"required"`
	SnapshotWindow             string            `yaml:"snapshotWindow" json:"-" validate:"required"`
	SnapshotRetentionLimit     int               `yaml:"snapshotRetentionLimit" json:"-"`
	AutomaticFailoverEnabled   bool              `yaml:"automaticFailoverEnabled" json:"-"`
	ApprovedMajorVersions      []string          `yaml:"approvedMajorVersions" json:"-"`
}

// CheckVersion verifies that a specific version chosen by the user for a new
// redis instances is valid and supported in the chosen plan.
func (p RedisPlan) CheckVersion(version string) bool {
	// Return true if there are no valid major versions set in the plan; this
	// lets the calls proceed and the AWS API will error out if an invalid
	// version is provided.
	if len(p.ApprovedMajorVersions) == 0 {
		return true
	}

	for _, approvedVersion := range p.ApprovedMajorVersions {
		if version == approvedVersion {
			return true
		}
	}

	return false
}

// ElasticsearchService describes the Elasticsearch Service. It contains the basic Service details as well as a list of Elasticsearch Plans
type ElasticsearchService struct {
	Service `yaml:",inline" validate:"required"`
	Plans   []ElasticsearchPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific ElasticsearchSecret Plan based on the plan ID.
func (s ElasticsearchService) FetchPlan(planID string) (ElasticsearchPlan, response.Response) {
	for _, plan := range s.Plans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return ElasticsearchPlan{}, response.NewErrorResponse(http.StatusBadRequest, ErrNoPlanFound.Error())
}

// ElasticsearchPlan inherits from a plan and adds fields needed for AWS Redis.
type ElasticsearchPlan struct {
	domain.ServicePlan         `yaml:",inline" validate:"required"`
	Tags                       map[string]string `yaml:"tags" json:"-" validate:"required" `
	ElasticsearchVersion       string            `yaml:"elasticsearchVersion" json:"-" validate:"required"`
	MasterCount                string            `yaml:"masterCount" json:"-"`
	DataCount                  string            `yaml:"dataCount" json:"-" validate:"required"`
	InstanceType               string            `yaml:"instanceType" json:"-" validate:"required"`
	MasterInstanceType         string            `yaml:"masterInstanceType" json:"-"`
	VolumeSize                 string            `yaml:"volumeSize" json:"-" validate:"required"`
	VolumeType                 string            `yaml:"volumeType" json:"-" validate:"required"`
	MasterEnabled              bool              `yaml:"masterEnabled" json:"-"`
	NodeToNodeEncryption       bool              `yaml:"nodeToNodeEncryption" json:"-"`
	EncryptAtRest              bool              `yaml:"encryptAtRest" json:"-"`
	AutomatedSnapshotStartHour string            `yaml:"automatedSnapshotStartHour" json:"-"`
	SubnetID1AZ1               string            `yaml:"subnetID1az1" json:"-" validate:"required"`
	SubnetID2AZ2               string            `yaml:"subnetID2az2" json:"-" validate:"required"`
	SubnetID3AZ1               string            `yaml:"subnetID3az1" json:"-" validate:"required"`
	SubnetID4AZ2               string            `yaml:"subnetID4az2" json:"-" validate:"required"`
	SecurityGroup              string            `yaml:"securityGroup" json:"-" validate:"required"`
	ApprovedMajorVersions      []string          `yaml:"approvedMajorVersions" json:"-"`
}

// CheckVersion verifies that a specific version chosen by the user for a new
// elasticsearch instances is valid and supported in the chosen plan.
func (p ElasticsearchPlan) CheckVersion(version string) bool {
	// Return true if there are no valid major versions set in the plan; this
	// lets the calls proceed and the AWS API will error out if an invalid
	// version is provided.
	if len(p.ApprovedMajorVersions) == 0 {
		return true
	}

	for _, approvedVersion := range p.ApprovedMajorVersions {
		if version == approvedVersion {
			return true
		}
	}

	return false
}

// Catalog struct holds a collections of services
type Catalog struct {
	// Instances of Services
	RdsService           RDSService           `yaml:"rds" json:"-"`
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
			services = append(services, catalogStruct.Field(i).Interface())
		}
	}
	return services
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
