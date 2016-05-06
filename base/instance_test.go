package base_test

import (
	"database/sql"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/common/request"
	"github.com/18F/aws-broker/common/response"
	"github.com/18F/aws-broker/db"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"github.com/ory-am/dockertest"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func baseInstanceTest(t *testing.T, DB *gorm.DB) {

	// Test create of database table
	assert.False(t, DB.HasTable(&base.Instance{}))
	db.MigrateDB(DB)
	assert.True(t, DB.HasTable(&base.Instance{}))
	// Test database name
	assert.Equal(t, "instances", DB.NewScope(base.Instance{}).TableName())

	// Test column names
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("uuid"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("service_id"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("plan_id"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("organization_guid"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("space_guid"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("host"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("port"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("state"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("created_at"))
	assert.True(t, DB.NewScope(base.Instance{}).HasColumn("updated_at"))

	_, resp := base.FindBaseInstance(DB, "my-uuid")
	assert.Equal(t, response.NewErrorResponse(http.StatusNotFound, gorm.RecordNotFound.Error()), resp)
	instance := base.Instance{
		UUID:    "my-uuid",
		Host:    "myhost@domain.com",
		Port:    1234,
		State:   base.InstanceNotCreated,
		Request: request.Request{},
	}
	DB.NewRecord(instance)
	DB.Create(&instance)
	i, resp := base.FindBaseInstance(DB, "my-uuid")
	// Make time equal for precision loss.
	instance.CreatedAt = i.CreatedAt
	instance.UpdatedAt = i.UpdatedAt
	assert.Nil(t, resp)
	assert.Equal(t, instance, i)
}

func TestBaseInstanceMySQL(t *testing.T) {
	var DB gorm.DB
	var c dockertest.ContainerID
	var err error
	if c, err = dockertest.ConnectToMySQL(60, time.Second, func(url string) bool {
		dbSql, err := sql.Open("mysql", url+"?charset=utf8&parseTime=True")
		if err != nil {
			return false
		}
		if dbSql.Ping() == nil {
			DB, _ = gorm.Open("mysql", dbSql)
			return true
		}
		return false
	}); err != nil {
		t.Fatalf("Could not connect to database: %s", err)
	}
	// DB.LogMode(true)
	defer c.KillRemove()
	baseInstanceTest(t, &DB)
}

func TestBaseInstancePostgresSQL(t *testing.T) {
	var DB gorm.DB
	var c dockertest.ContainerID
	var err error
	if c, err = dockertest.ConnectToPostgreSQL(60, time.Second, func(url string) bool {
		dbSql, err := sql.Open("postgres", url)
		if err != nil {
			return false
		}
		if dbSql.Ping() == nil {
			DB, _ = gorm.Open("postgres", dbSql)
			return true
		}
		return false
	}); err != nil {
		t.Fatalf("Could not connect to database: %s", err)
	}
	// DB.LogMode(true)
	defer c.KillRemove()
	baseInstanceTest(t, &DB)
}
