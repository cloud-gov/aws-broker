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
	"regexp"
	"strconv"
	"fmt"
)

func getDatabase(t *testing.T, dbType string) (*dockertest.ContainerID, string, string, int, *gorm.DB) {
	var DB gorm.DB
	var dbURL string
	var dbIP string
	var dbPort int
	var container dockertest.ContainerID
	var err error
	switch dbType {
	case "mysql":
		container, err = dockertest.ConnectToMySQL(60, time.Second, func(url string) bool {
			dbSql, err := sql.Open("mysql", url)
			if err != nil {
				return false
			}
			if dbSql.Ping() == nil {
				dbURL = url
				re := regexp.MustCompile(`\(([0-9a-zA-Z.*]+):([[0-9]+)\)`)
				match := re.FindStringSubmatch(dbURL)
				dbIP = match[1]
				dbPort, _ = strconv.Atoi(match[2])
				DB, _ = gorm.Open("mysql", dbSql)
				return true
			}
			return false
		})
	case "postgres":
		container, err = dockertest.ConnectToPostgreSQL(60, time.Second, func(url string) bool {
			dbSql, err := sql.Open("postgres", url)
			if err != nil {
				return false
			}
			if dbSql.Ping() == nil {
				dbURL = url
				re := regexp.MustCompile(`([0-9a-zA-Z.*]+):([0-9]+)\/`)
				match := re.FindStringSubmatch(dbURL)
				dbIP = match[1]
				dbPort, _ = strconv.Atoi(match[2])
				DB, _ = gorm.Open("postgres", dbSql)
				return true
			}
			return false
		})
	default:
		return nil, "", "", 0, nil
	}

	if err != nil {
		t.Fatalf("Could not connect to database: %s", err)
	}
	//DB.LogMode(true)
	return &container, dbURL, dbIP, dbPort, &DB
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
	container, _, _, _, DB := getDatabase(t, "mysql")
	rdsSettings.AddRDSSetting(&catalog.RDSSetting{DB: DB, Config: common.DBConfig{}}, "my-plan-id")
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
	state, err = adapter.createDB(&Instance{Database: "db", Username: "user"}, "")
	assert.NotNil(t, err)
	assert.Equal(t, ErrMissingPassword, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test nil db conn
	adapter = sharedDBAdapter{SharedDbConn: nil}
	state, err = adapter.createDB(&Instance{Database: "db", Username: "user"}, "pw")
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

	// Test db conn gone bad mysql
	container, _, _, _, DB := getDatabase(t, "mysql")
	adapter = sharedDBAdapter{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	container.KillRemove()
	state, err = adapter.createDB(&Instance{Database: "db", Username: "user", DbType: "mysql"}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrCannotReachSharedDB, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test db conn gone bad postgres
	container, _, _, _, DB = getDatabase(t, "postgres")
	adapter = sharedDBAdapter{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	container.KillRemove()
	state, err = adapter.createDB(&Instance{Database: "db", Username: "user", DbType: "postgres"}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrCannotReachSharedDB, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test create db mysql
	var url, ip string
	var port int
	container, url, ip, port, DB = getDatabase(t, "mysql")
	_ = url
	adapter = sharedDBAdapter{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	state, err = adapter.createDB(&Instance{Database: "db", Username: "username", DbType: "mysql"}, "pw")
	//t.Log(url)
	assert.Nil(t, err)
	assert.Equal(t, base.InstanceReady, state)
	// Check the database and user
	mysqlAddr := fmt.Sprintf("username:pw@tcp(%s:%d)/db?charset=utf8&parseTime=True&loc=Local", ip, port)
	//t.Log(mysqlAddr)
	db, err := gorm.Open("mysql", mysqlAddr)
	assert.Equal(t, "db", db.CurrentDatabase())
	assert.Nil(t, err)
	assert.True(t, isDBConnectionAlive(&db))
	db.Close()
	container.KillRemove()

	// Test create db postgres
	container, url, ip, port, DB = getDatabase(t, "postgres")
	adapter = sharedDBAdapter{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	state, err = adapter.createDB(&Instance{Database: "db", Username: "username", DbType: "postgres"}, "pw")
	//t.Log(url)
	assert.Nil(t, err)
	assert.Equal(t, base.InstanceReady, state)
	// Check the database and user
	pqsqlAddr := fmt.Sprintf("dbname=db user=username password=pw host=%s sslmode=disable port=%d", ip, port)
	//t.Log(pqsqlAddr)
	pqDB, err := gorm.Open("postgres", pqsqlAddr)
	assert.Equal(t, "db", pqDB.CurrentDatabase())
	assert.Nil(t, err)
	assert.True(t, isDBConnectionAlive(&pqDB))
	pqDB.Close()
	container.KillRemove()

	// Test invalid db type
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
