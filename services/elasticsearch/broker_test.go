package elasticsearch

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/testutil"
	brokertags "github.com/cloud-gov/go-broker-tags"
)

func TestValidate(t *testing.T) {
	testCases := map[string]struct {
		options     ElasticsearchOptions
		settings    *config.Settings
		expectedErr bool
	}{
		"accepted volume type": {
			options: ElasticsearchOptions{
				VolumeType: "gp3",
			},
			settings:    &config.Settings{},
			expectedErr: false,
		},
		"invalid volume type": {
			options: ElasticsearchOptions{
				VolumeType: "io1",
			},
			settings:    &config.Settings{},
			expectedErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.options.Validate(test.settings)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestCreateInstance(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		planID              string
		instance            *ElasticsearchInstance
		catalog             *catalog.Catalog
		provisionDetails    domain.ProvisionDetails
		elasticsearchBroker *elasticsearchBroker
	}{
		"success": {
			planID: "123",
			instance: &ElasticsearchInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			provisionDetails: domain.ProvisionDetails{
				PlanID: "123",
			},
			elasticsearchBroker: &elasticsearchBroker{
				catalog: &catalog.Catalog{
					ElasticsearchService: catalog.ElasticsearchService{
						ElasticsearchPlans: []catalog.ElasticsearchPlan{
							{
								ServicePlan: domain.ServicePlan{
									ID: "123",
								},
							},
						},
					},
				},
				settings: &config.Settings{
					EncryptionKey: helpers.RandStr(32),
					Environment:   "test", // use the mock adapter
				},
				tagManager: &mocks.MockTagGenerator{},
				brokerDB:   brokerDB,
				adapter:    &mockElasticsearchAdapter{},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.elasticsearchBroker.CreateInstance(test.instance.Uuid, test.provisionDetails)

			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestModifyInstance(t *testing.T) {
	testCases := map[string]struct {
		options         ElasticsearchOptions
		existingVersion string
		expectedErrMsg  string
	}{
		"valid version accepted": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			existingVersion: "OpenSearch_1.3",
		},
		"version with other options rejected": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
				VolumeType:           "gp3",
			},
			existingVersion: "OpenSearch_1.3",
			expectedErrMsg:  "engine version upgrade cannot be combined with other configuration options",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDb, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			planId := "plan-123"
			instanceId := helpers.RandStr(10)
			serviceId := helpers.RandStr(10)

			existingInstance := &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: serviceId,
						PlanID:    planId,
					},
					Uuid: instanceId,
				},
				ElasticsearchVersion: test.existingVersion,
			}
			if err := brokerDb.Create(&base.Instance{Uuid: instanceId, Request: existingInstance.Request}).Error; err != nil {
				t.Fatal((err))
			}
			if err := brokerDb.Create(existingInstance).Error; err != nil {
				t.Fatal(err)
			}

			rawParams, _ := json.Marshal(test.options)
			updateDetails := domain.UpdateDetails{
				PlanID:        planId,
				RawParameters: rawParams,
			}

			broker := &elasticsearchBroker{
				brokerDB: brokerDb,
				catalog: &catalog.Catalog{
					ElasticsearchService: catalog.ElasticsearchService{
						ElasticsearchPlans: []catalog.ElasticsearchPlan{
							{
								ServicePlan: domain.ServicePlan{
									ID: planId,
								},
							},
						},
					},
				},
				settings: &config.Settings{
					EncryptionKey: helpers.RandStr(32),
					Environment:   "test",
				},
				tagManager: &mocks.MockTagGenerator{},
				adapter:    &mockElasticsearchAdapter{},
				logger:     slog.New(&testutil.MockLogHandler{}),
			}

			err = broker.ModifyInstance(instanceId, updateDetails)
			if test.expectedErrMsg != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", test.expectedErrMsg)
				}
				if !strings.Contains(err.Error(), test.expectedErrMsg) {
					t.Fatalf("expected error containing %q, got %q", test.expectedErrMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
			}

		})
	}
}

func TestLastOperation(t *testing.T) {
	testCases := map[string]struct {
		planID              string
		dbInstance          *ElasticsearchInstance
		expectedState       base.InstanceState
		tagManager          brokertags.TagManager
		settings            *config.Settings
		catalog             *catalog.Catalog
		asyncJobMsg         *asyncmessage.AsyncJobMsg
		pollDetails         domain.PollDetails
		createTestInstances bool
	}{
		"create": {
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
			dbInstance: &ElasticsearchInstance{
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
				JobType: base.CreateOp,
				JobState: asyncmessage.AsyncJobState{
					Message: "completed",
					State:   base.InstanceReady,
				},
			},
			expectedState:       base.InstanceReady,
			createTestInstances: true,
		},
		"modify": {
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
			dbInstance: &ElasticsearchInstance{
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
			expectedState:       base.InstanceReady,
			createTestInstances: true,
		},
		"delete": {
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
			dbInstance: &ElasticsearchInstance{
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
					State:   base.InstanceReady,
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDB, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			broker := &elasticsearchBroker{
				brokerDB:   brokerDB,
				catalog:    test.catalog,
				settings:   test.settings,
				tagManager: test.tagManager,
				adapter:    &mockElasticsearchAdapter{},
				logger:     slog.New(&testutil.MockLogHandler{}),
			}

			if test.createTestInstances {
				err = brokerDB.Create(&base.Instance{
					Uuid:    test.dbInstance.Uuid,
					Request: test.dbInstance.Request,
				}).Error
				if err != nil {
					t.Fatal(err)
				}

				err = brokerDB.Create(test.dbInstance).Error
				if err != nil {
					t.Fatal(err)
				}
			}

			if test.asyncJobMsg != nil {
				test.asyncJobMsg.BrokerId = test.dbInstance.ServiceID
				test.asyncJobMsg.InstanceId = test.dbInstance.Uuid

				err = brokerDB.Create(test.asyncJobMsg).Error
				if err != nil {
					t.Fatal(err)
				}
			}

			lastOperation, err := broker.LastOperation(test.dbInstance.Uuid, test.pollDetails)
			if err != nil {
				t.Fatal(err)
			}

			if lastOperation.State != test.expectedState.ToLastOperationState() {
				t.Errorf("expected: %s, got: %s", test.expectedState, lastOperation.State)
			}
		})
	}
}
