package request

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/cloud-gov/aws-broker/helpers/response"
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
	RawParameters    json.RawMessage `json:"parameters,omitempty" sql:"-"`
}

// ExtractRequest will look at the request body and parse it into a Request struct to be used programmatically.
func ExtractRequest(req *http.Request) (Request, response.Response) {
	var cr Request
	if req.Body == nil {
		return cr, response.ErrNoRequestBodyResponse
	}
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return cr, response.NewErrorResponse(http.StatusBadRequest, err.Error())
	}
	json.Unmarshal(body, &cr)
	return cr, nil
}
