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
	"github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/mocks"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/go-test/deep"
)

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

func TestLastOperation(t *testing.T) {
	testCases := map[string]struct {
		planID        string
		instance      *RedisInstance
		expectedState base.InstanceState
		tagManager    brokertags.TagManager
		settings      *config.Settings
		catalog       *catalog.Catalog
		asyncJobMsg   *jobs.AsyncJobMsg
		pollDetails   domain.PollDetails
		adapter       redisAdapter
	}{
		"create successful": {
			pollDetails: domain.PollDetails{
				OperationData: base.CreateOp.String(),
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			expectedState: base.InstanceReady,
			adapter:       &mockRedisAdapter{},
		},
		"modify successful": {
			pollDetails: domain.PollDetails{
				OperationData: base.ModifyOp.String(),
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			asyncJobMsg: &jobs.AsyncJobMsg{
				JobType: base.ModifyOp,
				JobState: jobs.AsyncJobState{
					Message: "completed",
					State:   base.InstanceReady,
				},
			},
			expectedState: base.InstanceReady,
			adapter:       &mockRedisAdapter{},
		},
		"modify in progress": {
			pollDetails: domain.PollDetails{
				OperationData: base.ModifyOp.String(),
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			asyncJobMsg: &jobs.AsyncJobMsg{
				JobType: base.ModifyOp,
				JobState: jobs.AsyncJobState{
					Message: "in progress",
					State:   base.InstanceInProgress,
				},
			},
			expectedState: base.InstanceInProgress,
			adapter:       &mockRedisAdapter{},
		},
		"delete successful": {
			pollDetails: domain.PollDetails{
				OperationData: base.DeleteOp.String(),
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			expectedState: base.InstanceGone,
			asyncJobMsg: &jobs.AsyncJobMsg{
				JobType: base.DeleteOp,
				JobState: jobs.AsyncJobState{
					Message: "completed",
					State:   base.InstanceGone,
				},
			},
			adapter: &mockRedisAdapter{},
		},
		"delete in progress": {
			pollDetails: domain.PollDetails{
				OperationData: base.DeleteOp.String(),
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			expectedState: base.InstanceInProgress,
			asyncJobMsg: &jobs.AsyncJobMsg{
				JobType: base.DeleteOp,
				JobState: jobs.AsyncJobState{
					Message: "in progress",
					State:   base.InstanceInProgress,
				},
			},
			adapter: &mockRedisAdapter{},
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
				adapter:    test.adapter,
			}

			err = brokerDB.Create(test.instance).Error
			if err != nil {
				t.Fatal(err)
			}

			if test.asyncJobMsg != nil {
				test.asyncJobMsg.BrokerId = test.instance.ServiceID
				test.asyncJobMsg.InstanceId = test.instance.Uuid
				err := brokerDB.Create(test.asyncJobMsg).Error
				if err != nil {
					t.Fatal(err)
				}
			}

			lastOperation, err := broker.LastOperation(test.instance.Uuid, test.pollDetails)

			if err != nil {
				t.Fatal(err)
			}

			if lastOperation.State != test.expectedState.ToLastOperationState() {
				t.Errorf("expected: %s, got: %s", test.expectedState, lastOperation.State)
			}
		})
	}
}
