package rds

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"errors"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"crypto/aes"
	"github.com/18F/aws-broker/helpers/request"
)

func TestRDSGetCredentials(t *testing.T) {
	// Test unknown type.
	unknownType := Instance{DbType: "mariadb"}
	credentials, err := unknownType.getCredentials("pw")
	assert.Nil(t, credentials)
	assert.Equal(t, errors.New("Cannot generate credentials for unsupported db type: " + unknownType.DbType), err)

	// Test MySQL
	mysqlType := Instance{DbType: "mysql", Instance: base.Instance{Host: "mydomain.com", Port: 2555}, Username: "user", Database: "db"}
	credentials, err = mysqlType.getCredentials("pw")
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{"db_name": "db", "host": "mydomain.com", "port": "2555", "password": "pw", "username": "user", "uri": "mysql://user:pw@mydomain.com:2555/db"}, credentials)
	assert.Equal(t, 6, len(credentials))

	// Test Postgres
	postgresType  := Instance{DbType: "postgres", Instance: base.Instance{Host: "mydomain.com", Port: 2555}, Username: "user", Database: "db"}
	credentials, err = postgresType.getCredentials("pw")
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{"db_name": "db", "host": "mydomain.com", "port": "2555", "password": "pw", "username": "user", "uri": "postgres://user:pw@mydomain.com:2555/db"}, credentials)
	assert.Equal(t, 6, len(credentials))
}

func TestRDSTableName(t *testing.T) {
	i := Instance{}
	assert.Equal(t, "r_d_s_instances", i.TableName())
}

func TestRDSInit(t *testing.T) {
	// Test case when for too short of encryption key
	i := Instance{}
	err := i.init("my-uuid",
					"my-org-guid",
					"my-space-guid",
					"my-service-guid",
					catalog.RDSPlan{Plan: catalog.Plan{ID: "my-plan-id"}, Adapter: "my-adapter"},
					&config.Settings{EncryptionKey:"a"})
	assert.NotNil(t, err)
	assert.IsType(t, *new(aes.KeySizeError), err)
	assert.Equal(t, "crypto/aes: invalid key size 1", err.Error())

	// Test case when the init should succeed. Min size for the encryption key.
	i = Instance{}
	err = i.init("my-uuid",
		"my-org-guid",
		"my-space-guid",
		"my-service-guid",
		catalog.RDSPlan{Plan: catalog.Plan{ID: "my-plan-id"}, Adapter: "my-adapter"},
		&config.Settings{EncryptionKey:"abcdefghijklmnop"})
	assert.Nil(t, err)
	// Have to set the generated fields
	expectedInstance :=Instance{ClearPassword: i.ClearPassword, Salt: i.Salt, Username: i.Username, Password: i.Password,
						Adapter: "my-adapter", Database: i.Database,
						Instance: base.Instance{UUID: "my-uuid", Request: request.Request{PlanID: "my-plan-id", SpaceGUID: "my-space-guid", ServiceID: "my-service-guid", OrganizationGUID: "my-org-guid"}}}
	assert.Equal(t, expectedInstance, i)
}

func TestGetPassword(t *testing.T) {
	// Test no Salt
	i := Instance{}
	_, err := i.getPassword("")
	assert.Equal(t, ErrNoSaltSet, err)

	// Test no password
	i = Instance{Salt: "salt"}
	_ , err = i.getPassword("")
	assert.Equal(t, ErrNoPassword, err)

	//Test bad password get because bad key.
	i = Instance{Salt:"D3u3P1XhMrY+igzANaTRvw==", Password:"p6Uti3eEzXn7HaH3Zs6lqpX2LUlYYiXwMQ=="}
	_, err = i.getPassword("abcdefghijklmno")
	assert.NotNil(t, err)
	assert.IsType(t, *new(aes.KeySizeError), err)
	assert.Equal(t, "crypto/aes: invalid key size 15", err.Error())

	// Test Successful Password Get
	i = Instance{Salt:"D3u3P1XhMrY+igzANaTRvw==", Password:"p6Uti3eEzXn7HaH3Zs6lqpX2LUlYYiXwMQ=="}
	pw, err := i.getPassword("abcdefghijklmnop")
	assert.Nil(t, err)
	assert.Equal(t, "dkxd2ok89b2a03qgg0c9jz73w", pw)
}