package catalog

import (
	"fmt"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"gopkg.in/yaml.v3"
)

// RDSService describes the RDS Service. It contains the basic Service details as well as a list of RDS Plans
type RDSServiceRaw struct {
	Service  `yaml:",inline" validate:"required"`
	RDSPlans []RDSPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

type RDSService struct {
	domain.Service `yaml:",inline" validate:"required"`
	RDSPlans       []RDSPlan
}

// FetchPlan will look for a specific RDS Plan based on the plan ID.
func (s *RDSService) FetchPlan(planID string) (RDSPlan, error) {
	for _, plan := range s.RDSPlans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return RDSPlan{}, ErrNoPlanFound
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (s *RDSService) UnmarshalYAML(node *yaml.Node) error {
	// Check if the node is a mapping (object)
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("expected a mapping node, got %v", node.Kind)
	}

	// Create an intermediate type to unmarshal the raw values
	// This avoids infinite recursion if you try to unmarshal directly into RDSService
	var raw RDSServiceRaw
	// Decode the node into the rawCustomType
	if err := node.Decode(&raw); err != nil {
		return err
	}

	// Apply custom logic and assign values to CustomType fields
	if len(raw.RDSPlans) > 0 {
		for _, rdsPlan := range raw.RDSPlans {
			s.Plans = append(s.Plans, rdsPlan.ServicePlan)
		}
	}
	s.Service.ID = raw.Service.ID
	s.Service.Name = raw.Service.Name

	return nil
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
