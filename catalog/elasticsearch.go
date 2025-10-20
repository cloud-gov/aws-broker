package catalog

import (
	"code.cloudfoundry.org/brokerapi/v13/domain"
)

// ElasticsearchService describes the Elasticsearch Service. It contains the basic Service details as well as a list of Elasticsearch Plans
type ElasticsearchService struct {
	domain.Service `yaml:",inline" validate:"required"`
	// Plans   []ElasticsearchPlan `yaml:"plans" json:"plans" validate:"required,dive,required"`
}

// FetchPlan will look for a specific ElasticsearchSecret Plan based on the plan ID.
// func (s ElasticsearchService) FetchPlan(planID string) (ElasticsearchPlan, response.Response) {
// 	for _, plan := range s.Plans {
// 		if plan.ID == planID {
// 			return plan, nil
// 		}
// 	}
// 	return ElasticsearchPlan{}, response.NewErrorResponse(http.StatusBadRequest, ErrNoPlanFound.Error())
// }

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
