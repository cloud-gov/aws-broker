package response

import (
	"encoding/json"
	"net/http"
	"testing"
)

type responseTest struct {
	givenResponse      Response
	expectedJSONString string
	expectedStatusCode int
	expectedType       Type
}

var responseTests = []responseTest{
	{SuccessCreateResponse, "{\"description\":\"The instance was created\"}", http.StatusCreated, SuccessCreateResponseType},
	{SuccessDeleteResponse, "{\"description\":\"The instance was deleted\"}", http.StatusOK, SuccessDeleteResponseType},
	{NewErrorResponse(http.StatusNotFound, "oops"), "{\"description\":\"oops\"}", http.StatusNotFound, ErrorResponseType},
	{NewSuccessBindResponse(map[string]string{"username": "myuser"}), "{\"credentials\":{\"username\":\"myuser\"}}", http.StatusCreated, SuccessBindResponseType},
}

func TestGenericSuccessResponse(t *testing.T) {
	for _, test := range responseTests {
		b, err := json.Marshal(test.givenResponse)
		if err != nil {
			t.Error("Unable to convert struct to json")
		}
		jsonStr := string(b)
		if jsonStr != test.expectedJSONString {
			t.Error("Different JSON strings: expected " + test.expectedJSONString + " found " + jsonStr)
		}
		if test.givenResponse.GetStatusCode() != test.expectedStatusCode {
			t.Error("Different status codes: expected " + string(test.expectedStatusCode) + " found " + string(test.givenResponse.GetStatusCode()))
		}
		if test.givenResponse.GetResponseType() != test.expectedType {
			t.Error("Different response types: expected " + test.expectedType + " found " + test.givenResponse.GetResponseType())
		}
	}

}
