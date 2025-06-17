package redis

import (
	"net/http"
	"testing"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	"github.com/cloud-gov/aws-broker/services/rds"
	"github.com/cloud-gov/aws-broker/taskqueue"
	"github.com/jinzhu/gorm"
)

func testDBInit() (*gorm.DB, error) {
	config, err := common.InitTestDbConfig()
	if err != nil {
		return nil, err
	}
	db, err := common.DBInit(config)
	if err != nil {
		return nil, err
	}
	// Automigrate!
	db.AutoMigrate(&rds.RDSInstance{}, &RedisInstance{}, &elasticsearch.ElasticsearchInstance{}, &base.Instance{}, &taskqueue.AsyncJobMsg{}) // Add all your models here to help setup the database tables
	return db, err
}

func TestCreateInstance(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		planID               string
		instance             *RedisInstance
		expectedResponseCode int
		catalog              *catalog.Catalog
		createRequest        request.Request
		redisBroker          *redisBroker
	}{
		"success": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					Plans: []catalog.RedisPlan{
						{
							Plan: catalog.Plan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			createRequest: request.Request{
				PlanID: "123",
			},
			redisBroker: &redisBroker{
				settings: &config.Settings{
					EncryptionKey: helpers.RandStr(32),
					Environment:   "test", // use the mock adapter
				},
				tagManager: &mocks.MockTagGenerator{},
				brokerDB:   brokerDB,
			},
			expectedResponseCode: http.StatusAccepted,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			response := test.redisBroker.CreateInstance(test.catalog, test.instance.Uuid, test.createRequest)

			if response.GetStatusCode() != test.expectedResponseCode {
				t.Errorf("expected: %d, got: %d", test.expectedResponseCode, response.GetStatusCode())
			}
		})
	}
}
