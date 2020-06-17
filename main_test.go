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
	"github.com/18F/aws-broker/services/rds"
)

var createInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"organization_guid":"an-org",
	"space_guid":"a-space"
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

func TestCreateInstance(t *testing.T) {
	urlUnacceptsIncomplete := "/v2/service_instances/the_instance"
	resp, _ := doRequest(nil, urlUnacceptsIncomplete, "PUT", true, bytes.NewBuffer(createInstanceReq))

	if resp.Code != http.StatusUnprocessableEntity {
		t.Logf("Unable to create instance. Body is: " + resp.Body.String())
		t.Error(urlUnacceptsIncomplete, "with auth should return 422 and it returned", resp.Code)
	}

	urlAcceptsIncomplete := "/v2/service_instances/the_instance?accepts_incomplete=true"
	res, _ := doRequest(nil, urlAcceptsIncomplete, "PUT", true, bytes.NewBuffer(createInstanceReq))

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
	brokerDB.Where("uuid = ?", "the_instance").First(&i)
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

func TestBindInstance(t *testing.T) {
	url := "/v2/service_instances/the_instance/service_bindings/the_binding"
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	res, _ = doRequest(m, "/v2/service_instances/the_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 202 and it returned", res.Code)
	}

	res, _ = doRequest(m, url, "PUT", true, nil)
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
	brokerDB.Where("uuid = ?", "the_instance").First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestUnbind(t *testing.T) {
	url := "/v2/service_instances/the_instance/service_bindings/the_binding"
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

func TestDeleteInstance(t *testing.T) {
	url := "/v2/service_instances/the_instance"
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, "/v2/service_instances/the_instance?accepts_incomplete=true", "PUT", true, bytes.NewBuffer(createInstanceReq))
	i := rds.RDSInstance{}
	brokerDB.Where("uuid = ?", "the_instance").First(&i)
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
	brokerDB.Where("uuid = ?", "the_instance").First(&i)
	if len(i.Uuid) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}
