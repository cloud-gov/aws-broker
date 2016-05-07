package rds

import (
	"database/sql"
	"fmt"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/db"
	"github.com/18F/aws-broker/common/response"
	"github.com/jinzhu/gorm"
	"github.com/ory-am/dockertest"
	"github.com/stretchr/testify/assert"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"
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
			dbSQL, err := sql.Open("mysql", url)
			if err != nil {
				return false
			}
			if dbSQL.Ping() == nil {
				dbURL = url
				re := regexp.MustCompile(`\(([0-9a-zA-Z.*]+):([[0-9]+)\)`)
				match := re.FindStringSubmatch(dbURL)
				dbIP = match[1]
				dbPort, _ = strconv.Atoi(match[2])
				DB, _ = gorm.Open("mysql", dbSQL)
				return true
			}
			return false
		})
	case "postgres":
		container, err = dockertest.ConnectToPostgreSQL(60, time.Second, func(url string) bool {
			dbSQL, err := sql.Open("postgres", url)
			if err != nil {
				return false
			}
			if dbSQL.Ping() == nil {
				dbURL = url
				re := regexp.MustCompile(`([0-9a-zA-Z.*]+):([0-9]+)\/`)
				match := re.FindStringSubmatch(dbURL)
				dbIP = match[1]
				dbPort, _ = strconv.Atoi(match[2])
				DB, _ = gorm.Open("postgres", dbSQL)
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

func TestInitializeAgent(t *testing.T) {
	a := DefaultDBAdapter{}
	// Test Unknown Agent type
	dbAgent, resp := a.findBrokerAgent(catalog.RDSPlan{Agent: "ultimate"}, nil)
	assert.Nil(t, dbAgent)
	assert.Equal(t, ErrResponseAgentNotFound, resp)

	// Test Dedicated Agent Type
	dbAgent, resp = a.findBrokerAgent(catalog.RDSPlan{Agent: "dedicated"}, nil)
	assert.NotNil(t, dbAgent)
	assert.Nil(t, resp)
	assert.IsType(t, new(dedicatedAgent), dbAgent)

	// Test Shared Agent No Catalog
	dbAgent, resp = a.findBrokerAgent(catalog.RDSPlan{Agent: "shared"}, nil)
	assert.Nil(t, dbAgent)
	assert.Equal(t, ErrResponseCatalogNotFound, resp)

	// Test Shared Agent No RDS Settings
	dbAgent, resp = a.findBrokerAgent(catalog.RDSPlan{Agent: "shared"}, &catalog.Catalog{})
	assert.Nil(t, dbAgent)
	assert.Equal(t, ErrResponseRDSSettingsNotFound, resp)

	// Test Shared Agent No Plan
	c := &catalog.Catalog{}
	c.SetResources(catalog.Resources{RdsSettings: &catalog.RDSSettings{}})

	dbAgent, resp = a.findBrokerAgent(catalog.RDSPlan{Agent: "shared"}, c)
	assert.Nil(t, dbAgent)
	assert.Equal(t, response.NewErrorResponse(http.StatusInternalServerError, catalog.ErrNoRDSSettingForID.Error()), resp)

	// Test Shared Agent No DB in Plan
	c = &catalog.Catalog{}
	rdsSettings := &catalog.RDSSettings{}
	rdsSettings.AddRDSSetting(&catalog.RDSSetting{DB: nil, Config: db.Config{}}, "my-plan-id")
	c.SetResources(catalog.Resources{RdsSettings: rdsSettings})
	dbAgent, resp = a.findBrokerAgent(catalog.RDSPlan{Agent: "shared", Plan: catalog.Plan{ID: "my-plan-id"}}, c)
	assert.Nil(t, dbAgent)
	assert.Equal(t, ErrResponseDBNotFound, resp)

	// Test Shared Agent
	c = &catalog.Catalog{}
	rdsSettings = &catalog.RDSSettings{}
	container, _, _, _, DB := getDatabase(t, "mysql")
	rdsSettings.AddRDSSetting(&catalog.RDSSetting{DB: DB, Config: db.Config{}}, "my-plan-id")
	c.SetResources(catalog.Resources{RdsSettings: rdsSettings})
	dbAgent, resp = a.findBrokerAgent(catalog.RDSPlan{Agent: "shared", Plan: catalog.Plan{ID: "my-plan-id"}}, c)
	assert.NotNil(t, dbAgent)
	assert.IsType(t, new(sharedAgent), dbAgent)
	assert.Nil(t, resp)
	container.KillRemove()
}

func TestSharedDbCreateDb(t *testing.T) {
	// Test nil instance case
	agent := sharedAgent{SharedDbConn: nil}
	state, err := agent.createDB(nil, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrInstanceNotFound, err)
	assert.Equal(t, state, base.InstanceNotCreated)

	// Test no password case
	agent = sharedAgent{SharedDbConn: nil}
	state, err = agent.createDB(&Instance{Database: "db", Username: "user"}, "")
	assert.NotNil(t, err)
	assert.Equal(t, ErrMissingPassword, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test nil db conn
	agent = sharedAgent{SharedDbConn: nil}
	state, err = agent.createDB(&Instance{Database: "db", Username: "user"}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrDatabaseNotFound, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test bad db conn
	/*
		agent = sharedDBAgent{SharedDbConn: &gorm.DB{}}
		state, err = agent.createDB(&Instance{}, "pw")
		assert.NotNil(t, err)
		assert.Equal(t, ErrDatabaseNotFound, err)
		assert.Equal(t, base.InstanceNotCreated, state)
	*/

	// Test db conn gone bad mysql
	container, _, _, _, DB := getDatabase(t, "mysql")
	agent = sharedAgent{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	container.KillRemove()
	state, err = agent.createDB(&Instance{Database: "db", Username: "user", DbType: "mysql"}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrCannotReachSharedDB, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test db conn gone bad postgres
	container, _, _, _, DB = getDatabase(t, "postgres")
	agent = sharedAgent{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	container.KillRemove()
	state, err = agent.createDB(&Instance{Database: "db", Username: "user", DbType: "postgres"}, "pw")
	assert.NotNil(t, err)
	assert.Equal(t, ErrCannotReachSharedDB, err)
	assert.Equal(t, base.InstanceNotCreated, state)

	// Test create db mysql
	var url, ip string
	var port int
	container, url, ip, port, DB = getDatabase(t, "mysql")
	_ = url
	agent = sharedAgent{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	state, err = agent.createDB(&Instance{Database: "db", Username: "username", DbType: "mysql"}, "pw")
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
	agent = sharedAgent{SharedDbConn: DB}
	// Make sure it's live
	assert.Nil(t, DB.DB().Ping())
	// Remove the database
	state, err = agent.createDB(&Instance{Database: "db", Username: "username", DbType: "postgres"}, "pw")
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

// MockDBAgent is a struct meant for testing.
// It should only be used in *_test.go files.
// It is only here because *_test.go files are only compiled during "go test"
// and it's referenced in non *_test.go code eg. InitializeAgent in main.go.
type mockDBAgent struct {
}

func (d *mockDBAgent) createDB(i *Instance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockDBAgent) bindDBToApp(i *Instance, password string) (map[string]string, error) {
	// TODO
	return i.getCredentials(password)
}

func (d *mockDBAgent) deleteDB(i *Instance) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}
