package main

import (
	"bytes"
	"strings"

	"github.com/go-martini/martini"
	"github.com/jinzhu/gorm"

	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/18F/aws-broker/common"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/db"
	"github.com/18F/aws-broker/services/elasticsearch"
	"github.com/18F/aws-broker/services/rds"
	"github.com/18F/aws-broker/services/redis"
)

var (
	originalRDSPlanID                = "da91e15c-98c9-46a9-b114-02b8d28062c6"
	originalRDSMySQLPlanID           = "da91e15c-98c9-46a9-b114-02b8d28062c7"
	updateableRDSPlanID              = "1070028c-b5fb-4de8-989b-4e00d07ef5e8"
	nonUpdateableRDSPlan             = "ee75aef3-7697-4906-9330-fb1f83d719be"
	originalRedisPlanID              = "475e36bf-387f-44c1-9b81-575fec2ee443"
	nonUpdateableRedisPlanID         = "5nd336bf-0k7f-44c1-9b81-575fp3k764r6"
	originalElasticsearchPlanID      = "55b529cf-639e-4673-94fd-ad0a5dafe0ad"
	nonUpdateableElasticsearchPlanID = "162ffae8-9cf8-4806-80e5-a7f92d514198"
)

// micro-psql plan
var createRDSInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSMySQLInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c7",
	"parameters": {
		"version": "5.7"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSLatestMySQLInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c7",
	"parameters": {
		"version": "8.0"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

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

var createRDSPGWithInvaildVersionInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"version": "8"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

// micro-psql plan but with parameters
var modifyRDSInstanceReqStorage = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"storage": 15
	  },
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

// medium-psql plan
var modifyRDSInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"1070028c-b5fb-4de8-989b-4e00d07ef5e8",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "da91e15c-98c9-46a9-b114-02b8d28062c6"
	}
}`)

// medium-psql-redundant plan
var modifyRDSInstanceNotAllowedReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"ee75aef3-7697-4906-9330-fb1f83d719be",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "da91e15c-98c9-46a9-b114-02b8d28062c6"
	}
}`)

var createRedisInstanceReq = []byte(
	`{
	"service_id":"cda65825-e357-4a93-a24b-9ab138d97815",
	"plan_id":"475e36bf-387f-44c1-9b81-575fec2ee443",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRedisInstanceReq = []byte(
	`{
	"service_id":"cda65825-e357-4a93-a24b-9ab138d97815",
	"plan_id":"5nd336bf-0k7f-44c1-9b81-575fp3k764r6",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "475e36bf-387f-44c1-9b81-575fec2ee443"
	}
}`)

var createElasticsearchInstanceAdvancedOptionsReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"55b529cf-639e-4673-94fd-ad0a5dafe0ad",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"parameters": {
		"advanced_options": {
			"indices.query.bool.max_clause_count": "1024",
			"indices.fielddata.cache.size": "80"
		}
	}
}`)

var createElasticsearchInstanceReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"55b529cf-639e-4673-94fd-ad0a5dafe0ad",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyElasticsearchInstancePlanReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"162ffae8-9cf8-4806-80e5-a7f92d514198",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "55b529cf-639e-4673-94fd-ad0a5dafe0ad"
	}
}`)

var modifyElasticsearchInstanceParamsReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"55b529cf-639e-4673-94fd-ad0a5dafe0ad",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"parameters": {
		"advanced_options": {
			"indices.query.bool.max_clause_count": "1024",
			"indices.fielddata.cache.size": "80"
		}
	}
}`)

var brokerDB *gorm.DB

func setup() *martini.ClassicMartini {
	os.Setenv("AUTH_USER", "default")
	os.Setenv("AUTH_PASS", "default")
	var s config.Settings
	var dbConfig common.DBConfig
	s.DbConfig = &dbConfig
	dbConfig.DbType = "sqlite3"
	dbConfig.DbName = ":memory:"
	s.EncryptionKey = "12345678901234567890123456789012"
	s.Environment = "test"
	s.MaxAllocatedStorage = 1024
	brokerDB, _ = db.InternalDBInit(&dbConfig)

	m := App(&s, brokerDB)

	return m
}

/*
	Mock Objects
*/

func doRequest(m *martini.ClassicMartini, url string, method string, auth bool, body io.Reader) (*httptest.ResponseRecorder, *martini.ClassicMartini) {
	if m == nil {
		m = setup()
	}

	res := httptest.NewRecorder()
	req, _ := http.NewRequest(method, url, body)
	if auth {
		req.SetBasicAuth("default", "default")
	}

	m.ServeHTTP(res, req)

	return res, m
}

/*
	End Mock Objects
*/

func validJSON(response []byte, url string, t *testing.T) {
	var aJSON map[string]interface{}
	if json.Unmarshal(response, &aJSON) != nil {
		t.Error(url, "should return a valid json")
	}
}

func TestCatalog(t *testing.T) {
	url := "/v2/catalog"
	res, _ := doRequest(nil, url, "GET", false, nil)

	// Without auth
	if res.Code != http.StatusUnauthorized {
		t.Error(url, "without auth should return 401")
	}

	res, _ = doRequest(nil, url, "GET", true, nil)

	// With auth
	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)
}

/*
	Testing RDS
*/
func TestCreateRDSInstance(t *testing.T) {
	urlUnacceptsIncomplete := "/v2/service_instances/the_RDS_instance"
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(string(res.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Username == "" || i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}
}

func TestCreateRDSPGWithVersionInstance(t *testing.T) {
	urlUnacceptsIncomplete := "/v2/service_instances/the_RDS_instance"
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithVersionInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithVersionInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(string(res.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Username == "" || i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}
}

func TestCreateRDSPGWithInvaildVersionInstance(t *testing.T) {
	urlUnacceptsIncomplete := "/v2/service_instances/the_RDS_instance"
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithInvaildVersionInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithInvaildVersionInstanceReq))

	if res.Code != http.StatusBadRequest {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "...because the service plan does not allow updates or modification."?
	if !strings.Contains(string(res.Body.Bytes()), "is not a supported major version; major version must be one of:") {
		t.Error(urlAcceptsIncomplete, "should return a message that the version is invaild")
	}
}

func TestModifyRDSInstance(t *testing.T) {
	// We need to create an instance first before we can try to modify it.
	createURL := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := "/v2/service_instances/the_RDS_instance"
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReq))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(string(resp.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}

	// Reload the instance and check to see that the plan has been modified.
	i = rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.PlanID != updateableRDSPlanID {
		t.Logf("The instance was not modified: " + i.PlanID + " != " + updateableRDSPlanID)
		t.Error("The instance was not modified to have the new instance class plan.")
	}
}

func TestModifyRDSInstanceNotAllowed(t *testing.T) {
	// We need to create an instance first before we can try to modify it.
	createURL := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := "/v2/service_instances/the_RDS_instance"
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceNotAllowedReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceNotAllowedReq))

	if resp.Code != http.StatusBadRequest {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "...because the service plan does not allow updates or modification."?
	if !strings.Contains(string(resp.Body.Bytes()), "because the service plan does not allow updates or modification.") {
		t.Error(urlAcceptsIncomplete, "should return a message that the plan cannot be chosen")
	}

	// Reload the instance and check to see that the plan has not been modified.
	i = rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.PlanID != originalRDSPlanID {
		t.Logf("The instance was modified: " + i.PlanID + " != " + originalRDSPlanID)
		t.Error("The instance was modified to have a new instance class plan when it should not have been.")
	}
}

func TestModifyRDSInstanceSizeIncrease(t *testing.T) {
	// We need to create an instance first before we can try to modify it.
	createURL := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	println(i.AllocatedStorage)

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := "/v2/service_instances/the_RDS_instance"
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReqStorage))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	//Pull in AllocatedStorage and increase the storage
	urlAcceptsIncomplete := "/v2/service_instances/the_RDS_instance?accepts_incomplete=true"
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReqStorage))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	//Check to make sure storage size actually increased
	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(string(res.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and does it have correct storage?
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.AllocatedStorage != 15 {
		println(i.AllocatedStorage)
		t.Error("The Allocated Storage for the instance should be 15")
	}

}

func TestRDSLastOperation(t *testing.T) {
	url := "/v2/service_instances/the_RDS_instance/last_operation"
	res, m := doRequest(nil, url, "GET", true, bytes.NewBuffer(createRDSInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth status should be returned 404", res.Code)
	}

	// Create the instance and try again
	res, m = doRequest(m, "/v2/service_instances/the_RDS_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	// Check instance was created and StatusOK
	res, _ = doRequest(m, url, "GET", true, bytes.NewBuffer(createRDSInstanceReq))
	if res.Code != http.StatusOK {
		t.Logf("Unable to check last operation. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}
}

func TestRDSBindInstance(t *testing.T) {
	url := "/v2/service_instances/the_RDS_instance/service_bindings/the_binding"
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, "/v2/service_instances/the_RDS_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	res, _ = doRequest(m, url, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
	if res.Code != http.StatusCreated {
		t.Logf("Unable to bind instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	type credentials struct {
		Uri      string
		Username string
		Password string
		Host     string
		DbName   string
	}

	type response struct {
		Credentials credentials
	}

	var r response

	json.Unmarshal(res.Body.Bytes(), &r)

	// Does it contain "uri"
	if r.Credentials.Uri == "" {
		t.Error(url, "should return credentials")
	}

	instance := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestRDSUnbind(t *testing.T) {
	url := "/v2/service_instances/the_RDS_instance/service_bindings/the_binding"
	res, _ := doRequest(nil, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Is it an empty object?
	if string(res.Body.Bytes()) != "{}" {
		t.Error(url, "should return an empty JSON")
	}
}

func TestRDSDeleteInstance(t *testing.T) {
	url := "/v2/service_instances/the_RDS_instance"
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, "/v2/service_instances/the_RDS_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be in the DB")
	}

	res, _ = doRequest(m, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it actually gone from the DB?
	i = rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_RDS_instance").First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}

/*
	Testing Redis
*/

func TestCreateRedisInstance(t *testing.T) {
	urlUnacceptsIncomplete := "/v2/service_instances/the_redis_instance"
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_redis_instance?accepts_incomplete=true"
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(string(res.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata", i.PlanID, "plan", i.OrganizationGUID, "org", i.SpaceGUID)
	}
}

func TestModifyRedisInstance(t *testing.T) {
	// We need to create an instance first before we can try to modify it.
	createURL := "/v2/service_instances/the_redis_instance?accepts_incomplete=true"
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRedisPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := "/v2/service_instances/the_redis_instance"
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRedisInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_redis_instance?accepts_incomplete=true"
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRedisInstanceReq))

	if resp.Code != http.StatusBadRequest {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "Updating Redis service instances is not supported at this time"?
	if !strings.Contains(string(resp.Body.Bytes()), "Updating Redis service instances is not supported at this time") {
		t.Error(urlAcceptsIncomplete, "should return a message that Redis services cannot be modified at this time")
	}

	// Reload the instance and check to see that the plan has not been modified.
	i = redis.RedisInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&i)
	if i.PlanID != originalRedisPlanID {
		t.Logf("The instance was modified: " + i.PlanID + " != " + originalRedisPlanID)
		t.Error("The instance was modified to have a new instance class plan when it should not have been.")
	}
}

func TestRedisLastOperation(t *testing.T) {
	url := "/v2/service_instances/the_redis_instance/last_operation"
	res, m := doRequest(nil, url, "GET", true, bytes.NewBuffer(createRedisInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth status should be returned 404", res.Code)
	}

	// Create the instance and try again
	res, m = doRequest(m, "/v2/service_instances/the_redis_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	// Check instance was created and StatusOK
	res, _ = doRequest(m, url, "GET", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusOK {
		t.Logf("Unable to check last operation. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}
}

func TestRedisBindInstance(t *testing.T) {
	url := "/v2/service_instances/the_redis_instance/service_bindings/the_binding"
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, "/v2/service_instances/the_redis_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	res, _ = doRequest(m, url, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusCreated {
		t.Logf("Unable to bind instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	type credentials struct {
		Uri      string
		Username string
		Password string
		Host     string
		DbName   string
	}

	type response struct {
		Credentials credentials
	}

	var r response

	json.Unmarshal(res.Body.Bytes(), &r)

	// Does it contain "uri"
	if r.Credentials.Uri == "" {
		t.Error(url, "should return credentials")
	}

	instance := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestRedisUnbind(t *testing.T) {
	url := "/v2/service_instances/the_redis_instance/service_bindings/the_binding"
	res, _ := doRequest(nil, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Is it an empty object?
	if string(res.Body.Bytes()) != "{}" {
		t.Error(url, "should return an empty JSON")
	}
}

func TestRedisDeleteInstance(t *testing.T) {
	url := "/v2/service_instances/the_redis_instance"
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, "/v2/service_instances/the_redis_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be in the DB")
	}

	res, _ = doRequest(m, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it actually gone from the DB?
	i = redis.RedisInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}

/*
	Tests for elasticsearch
*/

func TestCreateElasticsearchInstance(t *testing.T) {
	urlUnacceptsIncomplete := "/v2/service_instances/the_elasticsearch_instance"
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true"
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(string(res.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata", i.PlanID, "plan", i.OrganizationGUID, "org", i.SpaceGUID)
	}

	if i.IndicesFieldDataCacheSize != "" {
		t.Error("The instance should not have IndicesFieldDataCacheSize but has it as", i.IndicesFieldDataCacheSize)
	}

	if i.IndicesQueryBoolMaxClauseCount != "" {
		t.Error("The instance should not have IndicesQueryBoolMaxClauseCount but has it as", i.IndicesQueryBoolMaxClauseCount)
	}

	urlAcceptsIncompleteAdv := "/v2/service_instances/the_advanced_elasticsearch_instance?accepts_incomplete=true"
	res, _ = doRequest(nil, urlAcceptsIncompleteAdv, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceAdvancedOptionsReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncompleteAdv, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncompleteAdv, t)

	// Does it say "accepted"?
	if !strings.Contains(string(res.Body.Bytes()), "accepted") {
		t.Error(urlAcceptsIncompleteAdv, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_advanced_elasticsearch_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata", i.PlanID, "plan", i.OrganizationGUID, "org", i.SpaceGUID)
	}

	if i.IndicesFieldDataCacheSize != "80" {
		t.Error("The instance should have IndicesFieldDataCacheSize 80 but has it as", i.IndicesFieldDataCacheSize)
	}

	if i.IndicesQueryBoolMaxClauseCount != "1024" {
		t.Error("The instance should have IndicesQueryBoolMaxClauseCount 1024 but has it as", i.IndicesQueryBoolMaxClauseCount)
	}
}

func TestModifyElasticsearchInstanceParams(t *testing.T) {
	// We need to create an instance first before we can try to modify it.
	createURL := "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true"
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalElasticsearchPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := "/v2/service_instances/the_elasticsearch_instance"
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstanceParamsReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true"
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstanceParamsReq))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.IndicesFieldDataCacheSize != "80" {
		t.Error("The instance should have IndicesFieldDataCacheSize 80 but has it as", i.IndicesFieldDataCacheSize)
	}

	if i.IndicesQueryBoolMaxClauseCount != "1024" {
		t.Error("The instance should have IndicesQueryBoolMaxClauseCount 1024 but has it as", i.IndicesQueryBoolMaxClauseCount)
	}

}

func TestModifyElasticsearchInstancePlan(t *testing.T) {
	// We need to create an instance first before we can try to modify it.
	createURL := "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true"
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalElasticsearchPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := "/v2/service_instances/the_elasticsearch_instance"
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstancePlanReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true"
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstancePlanReq))

	if resp.Code != http.StatusBadRequest {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "Updating Redis service instances is not supported at this time"?
	if !strings.Contains(string(resp.Body.Bytes()), "Updating Elasticsearch service instances is not supported at this time") {
		t.Error(urlAcceptsIncomplete, "should return a message that Elasticsearch services cannot be modified at this time")
	}

	// Reload the instance and check to see that the plan has not been modified.
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&i)
	if i.PlanID != originalElasticsearchPlanID {
		t.Logf("The instance was modified: " + i.PlanID + " != " + originalElasticsearchPlanID)
		t.Error("The instance was modified to have a new instance class plan when it should not have been.")
	}
}

func TestElasticsearchLastOperation(t *testing.T) {
	url := "/v2/service_instances/the_elasticsearch_instance/last_operation"
	res, m := doRequest(nil, url, "GET", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth status should be returned 404", res.Code)
	}

	// Create the instance and try again
	res, m = doRequest(m, "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	// Check instance was created and StatusOK
	res, _ = doRequest(m, url, "GET", true, bytes.NewBuffer(createElasticsearchInstanceReq))
	if res.Code != http.StatusOK {
		t.Logf("Unable to check last operation. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}
}

func TestElasticsearchBindInstance(t *testing.T) {
	url := "/v2/service_instances/the_elasticsearch_instance/service_bindings/the_binding"
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	res, _ = doRequest(m, url, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))
	if res.Code != http.StatusCreated {
		t.Logf("Unable to bind instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	type credentials struct {
		Uri      string
		Username string
		Password string
		Host     string
		DbName   string
	}

	type response struct {
		Credentials credentials
	}

	var r response

	json.Unmarshal(res.Body.Bytes(), &r)

	// Does it contain "uri"
	if r.Credentials.Uri == "" {
		t.Error(url, "should return credentials")
	}

	instance := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestElasticsearchUnbind(t *testing.T) {
	url := "/v2/service_instances/the_elasticsearch_instance/service_bindings/the_binding"
	res, _ := doRequest(nil, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Is it an empty object?
	if string(res.Body.Bytes()) != "{}" {
		t.Error(url, "should return an empty JSON")
	}
}

func TestElasticsearchDeleteInstance(t *testing.T) {
	url := "/v2/service_instances/the_elasticsearch_instance"
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, "/v2/service_instances/the_elasticsearch_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_elasticsearch_instance").First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be in the DB")
	}

	res, _ = doRequest(m, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it actually gone from the DB?
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", "the_redis_instance").First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}
