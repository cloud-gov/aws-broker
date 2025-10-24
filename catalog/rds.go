package catalog

import (
	"code.cloudfoundry.org/brokerapi/v13/domain"
)

type RDSService struct {
	Service  `yaml:",inline" validate:"required"`
	RDSPlans []RDSPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
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

func (s *RDSService) ToBrokerAPIService() domain.Service {
	service := domain.Service{
		ID:                   s.ID,
		Name:                 s.Name,
		Description:          s.Description,
		Bindable:             s.Bindable,
		InstancesRetrievable: s.InstancesRetrievable,
		BindingsRetrievable:  s.BindingsRetrievable,
		Tags:                 s.Tags,
		PlanUpdatable:        s.PlanUpdatable,
		Requires:             s.Requires,
		Metadata:             s.Metadata,
		DashboardClient:      s.DashboardClient,
		AllowContextUpdates:  s.AllowContextUpdates,
	}
	var plans []domain.ServicePlan
	for _, plan := range s.RDSPlans {
		plans = append(plans, plan.ServicePlan)
	}
	service.Plans = plans
	return service
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
