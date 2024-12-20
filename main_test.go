package main

import (
	"bytes"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/go-martini/martini"
	"github.com/google/uuid"
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
	"github.com/18F/aws-broker/taskqueue"
)

var (
	originalRDSPlanID           = "da91e15c-98c9-46a9-b114-02b8d28062c6"
	updateableRDSPlanID         = "1070028c-b5fb-4de8-989b-4e00d07ef5e8"
	originalRedisPlanID         = "475e36bf-387f-44c1-9b81-575fec2ee443"
	originalElasticsearchPlanID = "55b529cf-639e-4673-94fd-ad0a5dafe0ad"
)

// micro-psql plan
var createRDSInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSInstanceWithEnabledLogGroupsReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"parameters": {
	  "enable_cloudwatch_log_groups_exports": ["foo"]
	}
}`)

var createRDSPGWithVersionInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"version": "15"
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

var createRDSMySQLWithBinaryLogFormat = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"binary_log_format": "ROW"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSPostgreSQLWithEnablePgCron = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"enable_pg_cron": true
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
		"storage": 25
	  },
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRDSInstanceBinaryLogFormat = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"binary_log_format": "MIXED"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRDSInstanceEnablePgCron = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"enable_pg_cron": true
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRDSInstanceEnableCloudwatchLogGroups = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"enable_cloudwatch_log_groups_exports": ["foo"]
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

func initTestDbConfig() (*common.DBConfig, error) {
	var dbConfig common.DBConfig
	if dbConfig.DbType = os.Getenv("DB_TYPE"); dbConfig.DbType == "" {
		dbConfig.DbType = "sqlite3"
	}
	switch dbConfig.DbType {
	case "postgres":
		dbConfig.DbType = "postgres"
		dbConfig.DbName = os.Getenv("POSTGRES_USER")
		dbConfig.Password = os.Getenv("POSTGRES_PASSWORD")
		dbConfig.Sslmode = "disable"
		dbConfig.Port = 5432
		dbConfig.Username = os.Getenv("POSTGRES_USER")
		dbConfig.URL = "localhost"
	case "sqlite3":
		dbConfig.DbType = "sqlite3"
		dbConfig.DbName = ":memory:"
	default:
		return nil, fmt.Errorf("unsupported db type: %s", dbConfig.DbType)
	}
	return &dbConfig, nil
}

func initTestDb(dbConfig *common.DBConfig) (*gorm.DB, error) {
	brokerDB, err := db.InternalDBInit(dbConfig)
	if err != nil {
		return nil, err
	}
	return brokerDB, nil
}

func setup() *martini.ClassicMartini {
	os.Setenv("AUTH_USER", "default")
	os.Setenv("AUTH_PASS", "default")
	var s config.Settings

	dbConfig, err := initTestDbConfig()
	if err != nil {
		log.Fatal(err)
	}

	s.EncryptionKey = "12345678901234567890123456789012"
	s.Environment = "test"
	s.MaxAllocatedStorage = 1024
	s.CfApiUrl = "fake-api-url"
	s.CfApiClientId = "fake-client-id"
	s.CfApiClientSecret = "fake-client-secret"

	brokerDB, err = initTestDb(dbConfig)
	if err != nil {
		log.Fatal(err)
	}
	tq := taskqueue.NewQueueManager()
	tq.Init()

	m := App(&s, brokerDB, tq)

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
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithVersionInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithVersionInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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

func TestCreateRDSMySQLWithBinaryLogFormat(t *testing.T) {
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSMySQLWithBinaryLogFormat))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSMySQLWithBinaryLogFormat))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Username == "" || i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}

	if i.BinaryLogFormat != "ROW" {
		t.Error("The binary log format should be ROW")
	}
}

func TestCreateRDSPostgreSQLWithEnablePgCron(t *testing.T) {
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPostgreSQLWithEnablePgCron))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPostgreSQLWithEnablePgCron))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Username == "" || i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}

	if i.EnablePgCron != nil && !*i.EnablePgCron {
		t.Error("EnablePgCron should be true")
	}
}

func TestCreateRDSPGWithInvaildVersionInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithInvaildVersionInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSPGWithInvaildVersionInstanceReq))

	if res.Code != http.StatusBadRequest {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "...because the service plan does not allow updates or modification."?
	if !strings.Contains(res.Body.String(), "is not a supported major version; major version must be one of:") {
		t.Error(urlAcceptsIncomplete, "should return a message that the version is invaild")
	}
}

func TestCreateRDSInstanceWithEnabledLogGroups(t *testing.T) {
	instanceUUID := uuid.NewString()
	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRDSInstanceWithEnabledLogGroupsReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.Username == "" || i.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}

	if !slices.Contains(i.EnabledCloudWatchLogGroupExports, "foo") {
		t.Error("expected EnabledCloudWatchLogGroupExports to contain 'foo'")
	}
}

func TestModifyRDSInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReq))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(resp.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}

	// Reload the instance and check to see that the plan has been modified.
	i = rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.PlanID != updateableRDSPlanID {
		t.Logf("The instance was not modified: " + i.PlanID + " != " + updateableRDSPlanID)
		t.Error("The instance was not modified to have the new instance class plan.")
	}
}

func TestModifyRDSInstanceNotAllowed(t *testing.T) {
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceNotAllowedReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceNotAllowedReq))

	if resp.Code != http.StatusBadRequest {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "...because the service plan does not allow updates or modification."?
	if !strings.Contains(resp.Body.String(), "because the service plan does not allow updates or modification.") {
		t.Error(urlAcceptsIncomplete, "should return a message that the plan cannot be chosen")
	}

	// Reload the instance and check to see that the plan has not been modified.
	i = rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.PlanID != originalRDSPlanID {
		t.Logf("The instance was modified: " + i.PlanID + " != " + originalRDSPlanID)
		t.Error("The instance was modified to have a new instance class plan when it should not have been.")
	}
}

func TestModifyRDSInstanceSizeIncrease(t *testing.T) {
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	println(i.AllocatedStorage)

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReqStorage))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	// Pull in AllocatedStorage and increase the storage
	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceReqStorage))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Check to make sure storage size actually increased
	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and does it have correct storage?
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.AllocatedStorage != 25 {
		println(i.AllocatedStorage)
		t.Error("The Allocated Storage for the instance should be 25")
	}
}

func TestModifyBinaryLogFormat(t *testing.T) {
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceBinaryLogFormat))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	// Pull in AllocatedStorage and increase the storage
	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceBinaryLogFormat))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Check to make sure storage size actually increased
	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and does it have correct storage?
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.BinaryLogFormat != "MIXED" {
		t.Error("The binary log format for the instance should be MIXED")
	}
}

func TestModifyEnablePgCron(t *testing.T) {
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceEnablePgCron))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	// Pull in AllocatedStorage and increase the storage
	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceEnablePgCron))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Check to make sure storage size actually increased
	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and does it have correct storage?
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if i.EnablePgCron != nil && !*i.EnablePgCron {
		t.Error("EnablePgCron should be true")
	}
}

func TestModifyEnableCloudwatchLogGroups(t *testing.T) {
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRDSPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	// Pull in AllocatedStorage and increase the storage
	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ := doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRDSInstanceEnableCloudwatchLogGroups))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Check to make sure storage size actually increased
	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and does it have correct storage?
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance should be saved in the DB")
	}

	if !slices.Contains(i.EnabledCloudWatchLogGroupExports, "foo") {
		t.Error("expected EnabledCloudWatchLogGroupExports to contain 'foo'")
	}
}

func TestRDSLastOperation(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/last_operation", instanceUUID)
	res, m := doRequest(nil, url, "GET", true, bytes.NewBuffer(createRDSInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth status should be returned 404", res.Code)
	}

	// Create the instance and try again
	res, m = doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
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
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/the_binding", instanceUUID)
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createRDSInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
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
		URI      string
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
	if r.Credentials.URI == "" {
		t.Error(url, "should return credentials")
	}

	instance := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestRDSUnbind(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/the_binding", instanceUUID)
	res, _ := doRequest(nil, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Is it an empty object?
	if res.Body.String() != "{}" {
		t.Error(url, "should return an empty JSON")
	}
}

func TestRDSDeleteInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRDSInstanceReq))
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}

/*
	Testing Redis
*/

func TestCreateRedisInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	if !strings.Contains(res.Body.String(), "accepted") {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	}
	// Is it in the database and has a username and password?
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalRedisPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRedisInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyRedisInstanceReq))

	if resp.Code != http.StatusBadRequest {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "Updating Redis service instances is not supported at this time"?
	if !strings.Contains(resp.Body.String(), "Updating Redis service instances is not supported at this time") {
		t.Error(urlAcceptsIncomplete, "should return a message that Redis services cannot be modified at this time")
	}

	// Reload the instance and check to see that the plan has not been modified.
	i = redis.RedisInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.PlanID != originalRedisPlanID {
		t.Logf("The instance was modified: " + i.PlanID + " != " + originalRedisPlanID)
		t.Error("The instance was modified to have a new instance class plan when it should not have been.")
	}
}

func TestRedisLastOperation(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/last_operation", instanceUUID)
	res, m := doRequest(nil, url, "GET", true, bytes.NewBuffer(createRedisInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth status should be returned 404", res.Code)
	}

	// Create the instance and try again
	res, m = doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
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
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/the_binding", instanceUUID)
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
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
		URI      string
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
	if r.Credentials.URI == "" {
		t.Error(url, "should return credentials")
	}

	instance := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestRedisUnbind(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/the_binding", instanceUUID)
	res, _ := doRequest(nil, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Is it an empty object?
	if res.Body.String() != "{}" {
		t.Error(url, "should return an empty JSON")
	}
}

func TestRedisDeleteInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}

/*
	Tests for elasticsearch
*/

func TestCreateElasticsearchInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it say "accepted"?
	// if !strings.Contains(res.Body.String(), "accepted") {
	// 	t.Error(urlAcceptsIncomplete, "should return the instance accepted message")
	// }
	if res.Code != http.StatusAccepted {
		t.Error(urlAcceptsIncomplete, "should return the instance accepted")
	}

	// Is it in the database and has a username and password?
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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

	advancedInstanceUUID := uuid.NewString()
	urlAcceptsIncompleteAdv := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", advancedInstanceUUID)
	res, _ = doRequest(nil, urlAcceptsIncompleteAdv, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceAdvancedOptionsReq))

	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(urlAcceptsIncompleteAdv, "with auth should return 202 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), urlAcceptsIncompleteAdv, t)

	// Does it say "accepted"?
	// if !strings.Contains(res.Body.String(), "accepted") {
	// 	t.Error(urlAcceptsIncompleteAdv, "should return the instance accepted message")
	// }
	if res.Code != http.StatusAccepted {
		t.Error(urlAcceptsIncompleteAdv, "should return the instance accepted")
	}

	// Is it in the database and has a username and password?
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", advancedInstanceUUID).First(&i)
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
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalElasticsearchPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstanceParamsReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstanceParamsReq))

	if resp.Code != http.StatusAccepted {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 202 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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
	instanceUUID := uuid.NewString()
	// We need to create an instance first before we can try to modify it.
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res, m := doRequest(nil, createURL, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Check to make sure the request was successful.
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(createURL, "with auth should return 202 and it returned", res.Code)
	}

	// Check to make sure the instance was saved.
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.Uuid == "0" {
		t.Error("The instance was not saved to the DB.")
	}

	// Check to make sure the instance has the original plan set on it.
	if i.PlanID != originalElasticsearchPlanID {
		t.Error("The instance should have the plan provided with the create request.")
	}

	urlUnacceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	resp, _ := doRequest(m, urlUnacceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstancePlanReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	resp, _ = doRequest(m, urlAcceptsIncomplete, "PATCH", true, bytes.NewBuffer(modifyElasticsearchInstancePlanReq))

	if resp.Code != http.StatusBadRequest {
		t.Logf("Unable to modify instance. Body is: " + resp.Body.String())
		t.Error(urlAcceptsIncomplete, "with auth should return 400 and it returned", resp.Code)
	}

	// Is it a valid JSON?
	validJSON(resp.Body.Bytes(), urlAcceptsIncomplete, t)

	// Does it contain "Updating Redis service instances is not supported at this time"?
	if !strings.Contains(resp.Body.String(), "Updating Elasticsearch service instances is not supported at this time") {
		t.Error(urlAcceptsIncomplete, "should return a message that Elasticsearch services cannot be modified at this time")
	}

	// Reload the instance and check to see that the plan has not been modified.
	i = elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if i.PlanID != originalElasticsearchPlanID {
		t.Logf("The instance was modified: " + i.PlanID + " != " + originalElasticsearchPlanID)
		t.Error("The instance was modified to have a new instance class plan when it should not have been.")
	}
}

func TestElasticsearchLastOperation(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/last_operation", instanceUUID)
	res, m := doRequest(nil, url, "GET", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth status should be returned 404", res.Code)
	}

	// Create the instance and try again
	res, m = doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))
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
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/the_binding", instanceUUID)
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createElasticsearchInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
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
		URI      string
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
	if r.Credentials.URI == "" {
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
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/the_binding", instanceUUID)
	res, _ := doRequest(nil, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Is it an empty object?
	if res.Body.String() != "{}" {
		t.Error(url, "should return an empty JSON")
	}
}

func TestElasticsearchDeleteInstance(t *testing.T) {
	instanceUUID := uuid.NewString()
	url := fmt.Sprintf("/v2/service_instances/%s", instanceUUID)
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID), "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	i := elasticsearch.ElasticsearchInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
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
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}
