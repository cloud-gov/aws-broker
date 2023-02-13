package request

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"
)

func TestExtractRequest(t *testing.T) {
	var createRDSPGWithVersionInstanceReq = []byte(
		`{
		"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
		"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
		"parameters": {
			"version": "10"
		},
		"organization_guid":"an-org",
		"space_guid":"a-space"
	}`)

	body := bytes.NewBuffer(createRDSPGWithVersionInstanceReq)
	httpReq, _ := http.NewRequest("POST", "/foo", body)
	req, _ := ExtractRequest(httpReq)
	expectedReq := Request{
		ServiceID:        "db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
		PlanID:           "da91e15c-98c9-46a9-b114-02b8d28062c6",
		OrganizationGUID: "an-org",
		SpaceGUID:        "a-space",
		RawParameters: []byte(`{
			"version": "10"
		}`),
	}
	if !reflect.DeepEqual(req, expectedReq) {
		t.Errorf("expected: %+v, got %+v", expectedReq, req)
	}
}
