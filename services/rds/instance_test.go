package rds_test

import (
	"database/sql"
	"github.com/18F/aws-broker/services/rds"
	"github.com/18F/aws-broker/db"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"github.com/ory-am/dockertest"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func rdsInstanceTest(t *testing.T, DB *gorm.DB) {

	// Test create of database table
	assert.False(t, DB.HasTable(&rds.RDSInstance{}))
	db.MigrateDB(DB)
	assert.True(t, DB.HasTable(&rds.RDSInstance{}))
	// Test database name
	assert.Equal(t, "r_d_s_instances", DB.NewScope(rds.RDSInstance{}).TableName())

	// Test column names
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("uuid"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("service_id"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("plan_id"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("organization_guid"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("space_guid"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("host"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("port"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("state"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("created_at"))
	assert.True(t, DB.NewScope(rds.RDSInstance{}).HasColumn("updated_at"))
}

func TestRDSInstanceMySQL(t *testing.T) {
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
	rdsInstanceTest(t, &DB)
}

func TestRDSInstancePostgresSQL(t *testing.T) {
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
	rdsInstanceTest(t, &DB)
}

