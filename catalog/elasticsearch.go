package catalog

import (
	"errors"
	"strconv"

	"code.cloudfoundry.org/brokerapi/v13/domain"
)

// ElasticsearchService describes the Elasticsearch Service. It contains the basic Service details as well as a list of Elasticsearch Plans
type ElasticsearchService struct {
	Service            `yaml:",inline" validate:"required"`
	ElasticsearchPlans []ElasticsearchPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific ElasticsearchSecret Plan based on the plan ID.
func (s ElasticsearchService) FetchPlan(planID string) (ElasticsearchPlan, error) {
	for _, plan := range s.ElasticsearchPlans {
		if plan.ID == planID {
			return plan, nil
		}
	}
	return ElasticsearchPlan{}, ErrNoPlanFound
}

func (s *ElasticsearchService) ToBrokerAPIService() domain.Service {
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
	for _, plan := range s.ElasticsearchPlans {
		plans = append(plans, plan.ServicePlan)
	}
	service.Plans = plans
	return service
}

// ElasticsearchPlan inherits from a plan and adds fields needed for AWS Redis.
type ElasticsearchPlan struct {
	domain.ServicePlan         `yaml:",inline" validate:"required"`
	Tags                       map[string]string `yaml:"tags" json:"-" validate:"required" `
	ElasticsearchVersion       string            `yaml:"elasticsearchVersion" json:"-" validate:"required"`
	MasterCount                string            `yaml:"masterCount" json:"-"`
	DataCount                  string            `yaml:"dataCount" json:"-" validate:"required"`
	InstanceType               string            `yaml:"instanceType" json:"-" validate:"required"`
	InstanceSizeRank           int               `yaml:"instanceSizeRank" json:"-" validate:"required,gt=0"`
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

// dataCount returns the plan's configured data-node count as an int (0 if unset/invalid).
func (p ElasticsearchPlan) dataCount() int {
	n, _ := strconv.Atoi(p.DataCount)
	return n
}

// IsZoneAware reports whether the plan's domain is created with zone awareness
// enabled and spread across two subnets.
func (p ElasticsearchPlan) IsZoneAware() bool {
	return p.dataCount() > 1
}

// SizeRank returns a comparable number for the plan's overall size. It combines
// the instance-type rank with the data-node count so that, within the same
// instance type, more data nodes rank larger.
func (p ElasticsearchPlan) SizeRank() int {
	if p.InstanceSizeRank <= 0 {
		return -1
	}
	return p.InstanceSizeRank*100 + p.dataCount()
}

// CanUpgradeTo reports whether an instance currently on plan p may be updated to
// target. The rules are:
//   - HA status must match or be upgrade (HA -> HA, non-HA -> non-HA). Crossing
//     between HA and non-HA is only allowed for upgrading to HA,
//   - The target must be the same size or larger (no downgrades).
func (p ElasticsearchPlan) CanUpgradeTo(target ElasticsearchPlan) (bool, error) {
	if target.dataCount() < p.dataCount() {
		return false, errors.New("cannot reduce the number of data nodes; the target plan must have at least as many data nodes as the current plan (highly-available -ha plans run 4 data nodes and non-HA plans run 2)")
	}

	if !p.IsZoneAware() && target.IsZoneAware() {
		return false, errors.New("cannot move from a single-node plan to a multi-node plan; a single-node plan runs one data node on one subnet without zone awareness, so data nodes cannot be added in place")
	}

	from := p.SizeRank()
	to := target.SizeRank()
	if from < 0 || to < 0 {
		return false, errors.New("unable to determine plan sizes for the requested plan change")
	}
	if to < from {
		return false, errors.New("downgrading to a smaller plan is not supported; the target plan must be the same size or larger")
	}

	return true, nil
}
