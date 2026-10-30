package redis

import (
	"encoding/json"
	"net/http"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"code.cloudfoundry.org/brokerapi/v13/domain/apiresponses"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/mocks"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/go-test/deep"
	"github.com/google/uuid"
)

func TestCreateInstance(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		planID               string
		instance             *RedisInstance
		catalog              *catalog.Catalog
		redisBroker          *redisBroker
		provisionDetails     domain.ProvisionDetails
		expectedResponseCode int
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
		"invalid engine version": {
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			provisionDetails: domain.ProvisionDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine_version":"5.0"}`),
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
								Engine: "valkey",
								ApprovedEngineVersions: map[string][]string{
									"valkey": {"8.2"},
								},
							},
						},
					},
				},
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		"invalid engine version for engine from option": {
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			provisionDetails: domain.ProvisionDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine_version":"7.1", "engine": "valkey"}`),
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
								Engine: "valkey",
								ApprovedEngineVersions: map[string][]string{
									"redis":  {"7.1"},
									"valkey": {"8.2"},
								},
							},
						},
					},
				},
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		// Redis is no longer offered for creation: the engine option cannot
		// name an engine the plan does not provision.
		"engine from option is not the plan engine": {
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			provisionDetails: domain.ProvisionDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine": "redis"}`),
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
								Engine: "valkey",
								ApprovedEngineVersions: map[string][]string{
									"redis":  {"7.1"},
									"valkey": {"8.2"},
								},
							},
						},
					},
				},
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		"valid engine version for engine from option": {
			planID: "123",
			instance: &RedisInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			provisionDetails: domain.ProvisionDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine_version":"9.1", "engine": "valkey"}`),
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
								Engine: "valkey",
								ApprovedEngineVersions: map[string][]string{
									"redis":  {"7.1"},
									"valkey": {"9.1", "8.2"},
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
				if apiErr, ok := err.(*apiresponses.FailureResponse); ok {
					responseCode := apiErr.ValidatedStatusCode(nil)
					if responseCode != test.expectedResponseCode {
						t.Fatalf("response status code does not match, expected: %d, got %d", test.expectedResponseCode, responseCode)
					}
				} else {
					t.Fatal(err)
				}
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
		expectedInstance     *RedisInstance
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
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "456",
					},
				},
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
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
		"invalid engine version for current engine": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							ApprovedEngineVersions: map[string][]string{
								"redis": {"7.1"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
				},
				Engine: "redis",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine_version": "5.0"}`),
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		"invalid engine version for new engine from option": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1"},
								"valkey": {"8.2"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
				},
				Engine: "redis",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine": "valkey", "engine_version": "5.0"}`),
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		"invalid engine version for new engine from plan": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							ApprovedEngineVersions: map[string][]string{
								"redis": {"7.1"},
							},
						},
						{
							ServicePlan: domain.ServicePlan{
								ID: "456",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1"},
								"valkey": {"8.2"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID:        "456",
				RawParameters: json.RawMessage(`{"engine_version": "7.1"}`),
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		"valid engine version for new engine from option": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1"},
								"valkey": {"9.1", "8.2"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
				},
				Engine: "redis",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine": "valkey", "engine_version": "9.1"}`),
			},
		},
		"plan change retains the current engine": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1", "7.0"},
								"valkey": {"9.1", "8.2"},
							},
						},
						{
							ServicePlan: domain.ServicePlan{
								ID: "456",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1", "7.0"},
								"valkey": {"9.1", "8.2"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "456",
					},
				},
				Engine:        "redis",
				EngineVersion: "7.0",
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
					State: base.InstanceInProgress,
				},
				Engine:        "redis",
				EngineVersion: "7.0",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID: "123",
			},
		},
		// Redis can be retained but never adopted.
		"engine from option is neither the current nor the plan engine": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1"},
								"valkey": {"9.1", "8.2"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
				},
				Engine:        "valkey",
				EngineVersion: "8.2",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine": "redis", "engine_version": "7.1"}`),
			},
			expectedResponseCode: http.StatusBadRequest,
		},
		// An instance already running Redis may keep it.
		"engine from option matches the current engine": {
			catalog: &catalog.Catalog{
				RedisService: catalog.RedisService{
					RedisPlans: []catalog.RedisPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID: "123",
							},
							Engine: "valkey",
							ApprovedEngineVersions: map[string][]string{
								"redis":  {"7.1", "7.0"},
								"valkey": {"9.1", "8.2"},
							},
						},
					},
				},
			},
			redisInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
				},
				Engine:        "redis",
				EngineVersion: "7.0",
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
					State: base.InstanceInProgress,
				},
				Engine:        "redis",
				EngineVersion: "7.1",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			updateDetails: domain.UpdateDetails{
				PlanID:        "123",
				RawParameters: json.RawMessage(`{"engine": "redis", "engine_version": "7.1"}`),
			},
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
				if apiErr, ok := err.(*apiresponses.FailureResponse); ok {
					responseCode := apiErr.ValidatedStatusCode(nil)
					if responseCode != test.expectedResponseCode {
						t.Fatalf("response status code does not match, expected: %d, got %d", test.expectedResponseCode, responseCode)
					}
				} else {
					t.Fatal(err)
				}
			}

			updatedInstance := &RedisInstance{}
			err = broker.brokerDB.First(updatedInstance, "uuid = ?", test.redisInstance.Uuid).Error
			if err != nil {
				t.Fatal(err)
			}

			if test.expectedInstance != nil {
				test.expectedInstance.Uuid = test.redisInstance.Uuid
				if diff := deep.Equal(updatedInstance, test.expectedInstance); diff != nil {
					t.Error(diff)
				}
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
		asyncJobMsg   *asyncmessage.AsyncJobMsg
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
			asyncJobMsg: &asyncmessage.AsyncJobMsg{
				JobType: base.ModifyOp,
				JobState: asyncmessage.AsyncJobState{
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
			asyncJobMsg: &asyncmessage.AsyncJobMsg{
				JobType: base.ModifyOp,
				JobState: asyncmessage.AsyncJobState{
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
			asyncJobMsg: &asyncmessage.AsyncJobMsg{
				JobType: base.DeleteOp,
				JobState: asyncmessage.AsyncJobState{
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
			asyncJobMsg: &asyncmessage.AsyncJobMsg{
				JobType: base.DeleteOp,
				JobState: asyncmessage.AsyncJobState{
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

func TestValidateEngineAndVersion(t *testing.T) {
	testCases := map[string]struct {
		redisInstance        *RedisInstance
		expectedResponseCode int
		options              RedisOptions
		plan                 catalog.RedisPlan
		expectErr            bool
	}{
		"no engine version": {
			redisInstance: &RedisInstance{},
			options:       RedisOptions{},
			plan:          catalog.RedisPlan{},
		},
		"engine version is approved": {
			redisInstance: &RedisInstance{
				Engine: "valkey",
			},
			options: RedisOptions{
				EngineVersion: "9.1",
			},
			plan: catalog.RedisPlan{
				ApprovedEngineVersions: map[string][]string{
					"redis":  {"7.1", "7.0"},
					"valkey": {"9.1", "8.2"},
				},
			},
		},
		// Redis versions stay approved for instances already running Redis.
		"redis engine version is approved": {
			redisInstance: &RedisInstance{
				Engine: "redis",
			},
			options: RedisOptions{
				EngineVersion: "7.1",
			},
			plan: catalog.RedisPlan{
				ApprovedEngineVersions: map[string][]string{
					"redis":  {"7.1", "7.0"},
					"valkey": {"9.1", "8.2"},
				},
			},
		},
		"engine version is not approved": {
			redisInstance: &RedisInstance{
				Engine: "redis",
			},
			options: RedisOptions{
				EngineVersion: "5.0",
			},
			plan: catalog.RedisPlan{
				ApprovedEngineVersions: map[string][]string{
					"redis": {"7.1"},
				},
			},
			expectErr: true,
		},
		"engine version is not approved for the engine": {
			redisInstance: &RedisInstance{
				Engine: "valkey",
			},
			options: RedisOptions{
				EngineVersion: "7.1",
			},
			plan: catalog.RedisPlan{
				ApprovedEngineVersions: map[string][]string{
					"redis":  {"7.1"},
					"valkey": {"9.1", "8.2"},
				},
			},
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateEngineAndVersion(test.redisInstance, test.plan, test.options)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
		})
	}
}

func TestValidateEngine(t *testing.T) {
	valkeyPlan := catalog.RedisPlan{Engine: "valkey"}

	testCases := map[string]struct {
		currentEngine string
		plan          catalog.RedisPlan
		options       RedisOptions
		expectErr     bool
	}{
		"no engine option on create": {
			plan: valkeyPlan,
		},
		"no engine option on update": {
			currentEngine: "redis",
			plan:          valkeyPlan,
		},
		"plan engine on create": {
			plan:    valkeyPlan,
			options: RedisOptions{Engine: "valkey"},
		},
		// Redis is no longer offered for creation.
		"redis on create": {
			plan:      valkeyPlan,
			options:   RedisOptions{Engine: "redis"},
			expectErr: true,
		},
		"unknown engine on create": {
			plan:      valkeyPlan,
			options:   RedisOptions{Engine: "memcached"},
			expectErr: true,
		},
		// A pre-existing Redis instance may keep its engine...
		"current engine on update": {
			currentEngine: "redis",
			plan:          valkeyPlan,
			options:       RedisOptions{Engine: "redis"},
		},
		// ...and may move to the engine its plan provisions.
		"plan engine on update": {
			currentEngine: "redis",
			plan:          valkeyPlan,
			options:       RedisOptions{Engine: "valkey"},
		},
		// ...but a Valkey instance cannot move back to Redis.
		"redis on update from valkey": {
			currentEngine: "valkey",
			plan:          valkeyPlan,
			options:       RedisOptions{Engine: "redis"},
			expectErr:     true,
		},
		"unknown engine on update": {
			currentEngine: "redis",
			plan:          valkeyPlan,
			options:       RedisOptions{Engine: "memcached"},
			expectErr:     true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateEngine(test.currentEngine, test.plan, test.options)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
		})
	}
}
