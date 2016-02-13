package rds

import (
	"database/sql"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common"
	"github.com/18F/aws-broker/helpers/response"
	"github.com/jinzhu/gorm"
	"github.com/ory-am/dockertest"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func getDatabase(t *testing.T, dbType string) (*dockertest.ContainerID, string, *gorm.DB){
	var DB *gorm.DB
	var dbURL string
	var container dockertest.ContainerID
	var err error
	fn := func(url string) bool {
		dbURL = url
		dbSQL, err := sql.Open(dbType, url)
		if err != nil {
			return false
		}
		if dbSQL.Ping() == nil {
			DB, _ = gorm.Open(dbType, dbSQL)
			return true
		}
		return false
	}
	switch dbType {
	case "mysql":
		container, err = dockertest.ConnectToMySQL(60, time.Second, fn())
	case "postgres":
		container, err = dockertest.ConnectToPostgreSQL(60, time.Second, fn())
	default:
		return nil, "", nil
	}

	if err != nil {
		t.Fatalf("Could not connect to database: %s", err)
	}
	// DB.LogMode(true)
	return &container, dbURL, DB
}

func TestInitializeAdapter(t *testing.T) {
	// Test Unknown Adapter type
	dbAdapter, resp := initializeAdapter(catalog.RDSPlan{Adapter: "ultimate"}, nil)
	assert.Nil(t, dbAdapter)
	assert.Equal(t, ErrResponseAdapterNotFound, resp)

	// Test Dedicated Adapter Type
	dbAdapter, resp = initializeAdapter(catalog.RDSPlan{Adapter: "dedicated"}, nil)
	assert.NotNil(t, dbAdapter)
	assert.Nil(t, resp)
	assert.IsType(t, new(dedicatedDBAdapter), dbAdapter)

	// Test Shared Adapter No Catalog
	dbAdapter, resp = initializeAdapter(catalog.RDSPlan{Adapter: "shared"}, nil)
	assert.Nil(t, dbAdapter)
	assert.Equal(t, ErrResponseCatalogNotFound, resp)

	// Test Shared Adapter No RDS Settings
	dbAdapter, resp = initializeAdapter(catalog.RDSPlan{Adapter: "shared"}, &catalog.Catalog{})
	assert.Nil(t, dbAdapter)
	assert.Equal(t, ErrResponseRDSSettingsNotFound, resp)

	// Test Shared Adapter No Plan
	c := &catalog.Catalog{}
	c.SetResources(catalog.Resources{RdsSettings: &catalog.RDSSettings{}})
	dbAdapter, resp = initializeAdapter(catalog.RDSPlan{Adapter: "shared"}, c)
	assert.Nil(t, dbAdapter)
	assert.Equal(t, response.NewErrorResponse(http.StatusInternalServerError, catalog.ErrNoRDSSettingForID.Error()), resp)

	// Test Shared Adapter No DB in Plan
	c = &catalog.Catalog{}
	rdsSettings := &catalog.RDSSettings{}
	rdsSettings.AddRDSSetting(&catalog.RDSSetting{DB: nil, Config: common.DBConfig{}}, "my-plan-id")
	c.SetResources(catalog.Resources{RdsSettings: rdsSettings})
	dbAdapter, resp = initializeAdapter(catalog.RDSPlan{Adapter: "shared", Plan: catalog.Plan{ID: "my-plan-id"}}, c)
	assert.Nil(t, dbAdapter)
	assert.Equal(t, ErrResponseDBNotFound, resp)

	// Test Shared Adapter
	c = &catalog.Catalog{}
	rdsSettings = &catalog.RDSSettings{}
	var DB gorm.DB
	var container dockertest.ContainerID
	var err error
	if container, err = dockertest.ConnectToMySQL(60, time.Second, func(url string) bool {
		dbSQL, err := sql.Open("mysql", url)
		if err != nil {
			return false
		}
		if dbSQL.Ping() == nil {
			DB, _ = gorm.Open("mysql", dbSQL)
			return true
		}
		return false
	}); err != nil {
		t.Fatalf("Could not connect to database: %s", err)
	}
	// DB.LogMode(true)
	rdsSettings.AddRDSSetting(&catalog.RDSSetting{DB: &DB, Config: common.DBConfig{}}, "my-plan-id")
	c.SetResources(catalog.Resources{RdsSettings: rdsSettings})
	dbAdapter, resp = initializeAdapter(catalog.RDSPlan{Adapter: "shared", Plan: catalog.Plan{ID: "my-plan-id"}}, c)
	assert.NotNil(t, dbAdapter)
	assert.IsType(t, new(sharedDBAdapter), dbAdapter)
	assert.Nil(t, resp)
	container.KillRemove()
}

func TestSharedDbCreateDb(t *testing.T) {
	// Test nil instance case
	adapter := sharedDBAdapter{SharedDbConn: nil}
	state, err := adapter.createDB(nil, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrInstanceNotFound, err)
	assert.Equal(t, state, base.InstanceNotCreated)

	// Test no password case
	adapter = sharedDBAdapter{SharedDbConn: nil}
	state, err = adapter.createDB(&Instance{}, "")
	assert.NotNil(t, err)
	assert.Equal(t, ErrMissingPassword, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test nil db conn
	adapter = sharedDBAdapter{SharedDbConn: nil}
	state, err = adapter.createDB(&Instance{}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test bad db conn
	/*
	adapter = sharedDBAdapter{SharedDbConn: &gorm.DB{}}
	state, err = adapter.createDB(&Instance{}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
	assert.Equal(t, base.InstanceNotCreated, state)
	*/
}

// MockDBAdapter is a struct meant for testing.
// It should only be used in *_test.go files.
// It is only here because *_test.go files are only compiled during "go test"
// and it's referenced in non *_test.go code eg. InitializeAdapter in main.go.
type mockDBAdapter struct {
}

func (d *mockDBAdapter) createDB(i *Instance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockDBAdapter) bindDBToApp(i *Instance, password string) (map[string]string, error) {
	// TODO
	return i.getCredentials(password)
}

func (d *mockDBAdapter) deleteDB(i *Instance) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}
