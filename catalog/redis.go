package catalog

import (
	"code.cloudfoundry.org/brokerapi/v13/domain"
)

// RedisService describes the Redis Service. It contains the basic Service details as well as a list of Redis Plans
type RedisService struct {
	domain.Service `yaml:",inline" validate:"required"`
	// Plans   []RedisPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific RedisSecret Plan based on the plan ID.
// func (s RedisService) FetchPlan(planID string) (RedisPlan, response.Response) {
// 	for _, plan := range s.Plans {
// 		if plan.ID == planID {
// 			return plan, nil
// 		}
// 	}
// 	return RedisPlan{}, response.NewErrorResponse(http.StatusBadRequest, ErrNoPlanFound.Error())
// }

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
