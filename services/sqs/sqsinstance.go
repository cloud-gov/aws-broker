package sqs

import (
	"github.com/18F/aws-broker/base"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
)

// SQSInstance represents the information of a SQS Service instance.
type SQSInstance struct {
	base.Instance

	Tags                          map[string]string `sql:"-"`
	DelaySeconds                  string            `sql:"-"`
	MaximumMessageSize            string            `sql:"-"`
	MessageRetentionPeriod        string            `sql:"-"`
	Policy                        string            `sql:"-"`
	ReceiveMessageWaitTimeSeconds string            `sql:"-"`
	VisibilityTimeout             string            `sql:"-"`
}

func (i SQSInstance) init(uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.ServicePlan,
	s *config.Settings) error {

	i.Uuid = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	// Load tags
	i.Tags = plan.Tags

	// Load AWS values
	i.DelaySeconds = plan.SQSProperties.DelaySeconds
	i.MaximumMessageSize = plan.SQSProperties.MaximumMessageSize
	i.MessageRetentionPeriod = plan.SQSProperties.MessageRetentionPeriod
	i.Policy = plan.SQSProperties.Policy
	i.ReceiveMessageWaitTimeSeconds = plan.SQSProperties.ReceiveMessageWaitTimeSeconds
	i.VisibilityTimeout = plan.SQSProperties.VisibilityTimeout

	return nil
}
