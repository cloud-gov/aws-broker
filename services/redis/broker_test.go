package redis

import (
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/testutil"
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
