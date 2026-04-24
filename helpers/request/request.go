package request

import (
	"encoding/json"
)

// Request is the format of the body for all create instance requests.
//
//	{
//	  "service_id":        "service-guid-here",
//	  "plan_id":           "plan-guid-here",
//	  "organization_guid": "org-guid-here",
//	  "space_guid":        "space-guid-here"
//	}
type Request struct {
	ServiceID        string          `json:"service_id" sql:"size(255)"`
	PlanID           string          `json:"plan_id" sql:"size(255)"`
	OrganizationGUID string          `json:"organization_guid" sql:"size(255)"`
	SpaceGUID        string          `json:"space_guid" sql:"size(255)"`
	RawParameters    json.RawMessage `json:"parameters,omitempty" gorm:"-"`
}
