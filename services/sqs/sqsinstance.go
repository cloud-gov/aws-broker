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
	plan catalog.SQSPlan,
	s *config.Settings) error {

	i.Uuid = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	// Load AWS values
	i.DelaySeconds = plan.Properties.DelaySeconds
	i.MaximumMessageSize = plan.Properties.MaximumMessageSize
	i.MessageRetentionPeriod = plan.Properties.MessageRetentionPeriod
	i.Policy = plan.Properties.Policy
	i.ReceiveMessageWaitTimeSeconds = plan.Properties.ReceiveMessageWaitTimeSeconds
	i.VisibilityTimeout = plan.Properties.VisibilityTimeout

	return nil
}
