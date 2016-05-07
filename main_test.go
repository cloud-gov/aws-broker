package main

import (
	"github.com/jinzhu/gorm"

	"bytes"
	"encoding/json"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/common/config"
	"github.com/18F/aws-broker/common/db"
	"github.com/18F/aws-broker/common/env"
	"github.com/18F/aws-broker/services/rds"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

var createInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"44d24fc7-f7a4-4ac1-b7a0-de82836e89a3",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var catalogData = []byte(`
rds:
  id: "db80ca29-2d1b-4fbc-aad3-d03c0bfa7593"
  name: "rds"
  description: "RDS Database Broker"
  bindable: true
  tags:
    - "database"
    - "RDS"
    - "postgresql"
    - "mysql"
  metadata:
    displayName: RDS Database Broker
    imageUrl:
    longDescription:
    providerDisplayName: RDS
    documentationUrl:
    supportUrl:
  plans:
    -
      id: "44d24fc7-f7a4-4ac1-b7a0-de82836e89a3"
      name: "shared-psql"
      description: "Shared infrastructure for Postgres DB"
      metadata:
        bullets:
          - "Shared RDS Instance"
          - "Postgres instance"
        costs:
          -
            amount:
              usd: 0
            unit: "MONTHLY"
        displayName: "Free Shared Plan"
      free: true
      adapter: shared
      dbType: sqlite3
      securityGroup: sg-123456
      subnetGroup: subnet-group
      tags:
        environment: "cf-env"
        client: "the client"
        service: "aws-broker"
`)

var secretsData = []byte(`
rds:
  service_id: "db80ca29-2d1b-4fbc-aad3-d03c0bfa7593"
  plans:
  -
    plan_id: "44d24fc7-f7a4-4ac1-b7a0-de82836e89a3"
    url: "test"
    username: "theuser"
    password: "thepassword"
    db_name: "db_name"
    db_type: "sqlite3"
    ssl_mode: "disable"
    port: 55
`)

var brokerDB *gorm.DB

func setup() *gin.Engine {
	os.Setenv("AUTH_USER", "default")
	os.Setenv("AUTH_PASS", "default")
	var s env.SystemEnv
	var dbConfig db.Config
	s.DbConfig = dbConfig
	dbConfig.DbType = "sqlite3"
	dbConfig.DbName = ":memory:"
	s.EncryptionKey = "12345678901234567890123456789012"
	// Get the models to migrate.
	var models []interface{}
	models = append(models, new(base.Instance))
	models = append(models, new(rds.Instance))
	brokerDB, _ = db.InternalDBInit(dbConfig, models)

	r := App(&s, config.InitDefaultAppConfig(), brokerDB, catalogData, secretsData)

	return r
}

/*
	Mock Objects
*/

func doRequest(m *gin.Engine, url string, method string, auth bool, body io.Reader) (*httptest.ResponseRecorder, *gin.Engine) {
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
	t.SkipNow()
	url := "/v2/service_instances/the_instance"

	res, _ := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createInstanceReq))

	if res.Code != http.StatusCreated {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 201 and it returned", res.Code)
	}

	// Is it a valid JSON?
	validJSON(res.Body.Bytes(), url, t)

	// Does it say "created"?
	if !strings.Contains(string(res.Body.Bytes()), "created") {
		t.Error(url, "should return the instance created message")
	}

	// Is it in the database and has a username and password?
	i := base.Instance{}
	brokerDB.Where("uuid = ?", "the_instance").First(&i)
	if len(i.UUID) == 0 {
		t.Error("The instance should be saved in the DB")
	}

	if i.PlanID == "" || i.OrganizationGUID == "" || i.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}

	// Is it in the database and has a username and password?
	rdsInstance := rds.Instance{}
	brokerDB.Where("uuid = ?", "the_instance").First(&rdsInstance)
	if len(rdsInstance.UUID) == 0 {
		t.Error("The instance should be saved in the DB")
	}

	if rdsInstance.Username == "" || rdsInstance.Password == "" {
		t.Error("The instance should have a username and password")
	}

	if rdsInstance.PlanID == "" || rdsInstance.OrganizationGUID == "" || rdsInstance.SpaceGUID == "" {
		t.Error("The instance should have metadata")
	}

}

func TestBindInstance(t *testing.T) {
	t.SkipNow()
	url := "/v2/service_instances/the_instance/service_bindings/the_binding"
	res, m := doRequest(nil, url, "PUT", true, bytes.NewBuffer(createInstanceReq))

	// Without the instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, "/v2/service_instances/the_instance", "PUT", true, bytes.NewBuffer(createInstanceReq))

	res, _ = doRequest(m, url, "PUT", true, nil)
	if res.Code != http.StatusCreated {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 201 and it returned", res.Code)
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

	instance := rds.Instance{}
	brokerDB.Where("uuid = ?", "the_instance").First(&instance)

	// Does it return an unencrypted password?
	if instance.Password == r.Credentials.Password || r.Credentials.Password == "" {
		t.Error(url, "should return an unencrypted password and it returned", r.Credentials.Password)
	}
}

func TestUnbind(t *testing.T) {
	t.SkipNow()
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
	t.SkipNow()
	url := "/v2/service_instances/the_instance"
	res, m := doRequest(nil, url, "DELETE", true, nil)

	// With no instance
	if res.Code != http.StatusNotFound {
		t.Error(url, "with auth should return 404 and it returned", res.Code)
	}

	// Create the instance and try again
	doRequest(m, "/v2/service_instances/the_instance", "PUT", true, bytes.NewBuffer(createInstanceReq))
	i := rds.Instance{}
	brokerDB.Where("uuid = ?", "the_instance").First(&i)
	if len(i.UUID) == 0 {
		t.Error("The instance should be in the DB")
	}

	res, _ = doRequest(m, url, "DELETE", true, nil)

	if res.Code != http.StatusOK {
		t.Logf("Unable to create instance. Body is: " + res.Body.String())
		t.Error(url, "with auth should return 200 and it returned", res.Code)
	}

	// Is it actually gone from the DB?
	i = rds.Instance{}
	brokerDB.Where("uuid = ?", "the_instance").First(&i)
	if len(i.UUID) > 0 {
		t.Error("The instance shouldn't be in the DB")
	}
}
