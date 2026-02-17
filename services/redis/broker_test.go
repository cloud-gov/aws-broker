package redis

import (
	"net/http"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/testutil"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/go-test/deep"
	"gorm.io/gorm"
)

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	// Automigrate!
	db.AutoMigrate(&RedisInstance{}, &base.Instance{}, &jobs.AsyncJobMsg{})
	return db, err
}

func TestCreateInstance(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		planID           string
		instance         *RedisInstance
		catalog          *catalog.Catalog
		redisBroker      *redisBroker
		provisionDetails domain.ProvisionDetails
	}{
		"success": {
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			provisionDetails: domain.ProvisionDetails{
				PlanID: "123",
			},
			redisBroker: &redisBroker{
				settings: &config.Settings{
					EncryptionKey: helpers.RandStr(32),
					Environment:   "test", // use the mock adapter
				},
				tagManager: &mocks.MockTagGenerator{},
				adapter:    &mockRedisAdapter{},
				brokerDB:   brokerDB,
				catalog: &catalog.Catalog{
					RedisService: catalog.RedisService{
						RedisPlans: []catalog.RedisPlan{
							{
								ServicePlan: domain.ServicePlan{
									ID: "123",
								},
							},
						},
					},
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.redisBroker.CreateInstance(test.instance.Uuid, test.provisionDetails)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestModifyInstance(t *testing.T) {
	testCases := map[string]struct {
		redisInstance        *RedisInstance
		expectedResponseCode int
		tagManager           brokertags.TagManager
		settings             *config.Settings
		catalog              *catalog.Catalog
		modifyRequest        request.Request
		updateDetails        domain.UpdateDetails
		expectedDbInstance   *RedisInstance
	}{
		"success": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
						},
						{
							ServicePlan: domain.ServicePlan{
								ID: "456",
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "456",
					},
				},
			},
			expectedDbInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
					State: base.InstanceInProgress,
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID: "123",
			},
			expectedResponseCode: http.StatusAccepted,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			broker := &redisBroker{
				brokerDB:   brokerDB,
				catalog:    test.catalog,
				settings:   test.settings,
				tagManager: test.tagManager,
				adapter:    &mockRedisAdapter{},
			}

			err = brokerDB.Create(test.redisInstance).Error
			if err != nil {
				t.Fatal(err)
			}

			err = broker.ModifyInstance(test.redisInstance.Uuid, test.updateDetails)
			if err != nil {
				t.Fatal(err)
			}

			updatedInstance := &RedisInstance{}
			err = broker.brokerDB.First(updatedInstance, test.redisInstance.Uuid).Error
			if err != nil {
				t.Fatal(err)
			}

			if diff := deep.Equal(updatedInstance, test.expectedDbInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}
