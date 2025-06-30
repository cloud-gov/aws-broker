package redis

import (
	"net/http"
	"testing"

	taskqueue "github.com/cloud-gov/aws-broker/async_jobs"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
)

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	// Automigrate!
	db.AutoMigrate(&RedisInstance{}, &base.Instance{}, &taskqueue.AsyncJobMsg{})
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
