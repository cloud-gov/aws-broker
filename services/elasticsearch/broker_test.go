package elasticsearch

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"code.cloudfoundry.org/brokerapi/v13/domain/apiresponses"
	"github.com/aws/aws-sdk-go-v2/aws"
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
		options                  ElasticsearchOptions
		existingInstance         *ElasticsearchInstance
		updateDetails            domain.UpdateDetails
		existingVersion          string
		versionUpgradeInProgress bool
		expectedErrMsg           string
		expectedStatus           int
		targetPlanID             string
		expectedPlanID           string
		plans                    []catalog.ElasticsearchPlan
	}{
		"valid version accepted": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-123",
					},
					Uuid: helpers.RandStr(10),
				},
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			targetPlanID:   "plan-123",
			expectedPlanID: "plan-123",
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{ID: "plan-123"},
				},
			},
		},
		"version with other options rejected": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
				VolumeType:           "gp3",
			},
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-123",
					},
					Uuid: helpers.RandStr(10),
				},
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			expectedErrMsg: "engine version upgrade cannot be combined with other configuration options",
			targetPlanID:   "plan-123",
			expectedPlanID: "plan-123",
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{ID: "plan-123"},
				},
			},
		},
		"version with log publishing rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-123",
					},
					Uuid: helpers.RandStr(10),
				},
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
				LogPublishing:        ElasticsearchLogOptions{ErrorLogs: aws.Bool(true)},
			},
			targetPlanID:   "plan-123",
			expectedPlanID: "plan-123",
			expectedErrMsg: "engine version upgrade cannot be combined with other configuration options",
			expectedStatus: http.StatusBadRequest,
		},
		"non-HA upgrade to larger plan accepted": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-medium",
					},
					Uuid: helpers.RandStr(10),
				},
				ElasticsearchVersion: "OpenSearch_1.3",
				InstanceType:         "r7g.medium.search",
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "search-medium",
					},
					InstanceSizeRank: 20,
					DataCount:        "2",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-large",
						Name: "search-large",
					},
					InstanceType:     "r7g.large.search",
					InstanceSizeRank: 30,
					DataCount:        "2",
				},
			},
			targetPlanID:   "plan-large",
			expectedPlanID: "plan-large",
		},
		"non-HA downgrade rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-large",
					},
					Uuid: helpers.RandStr(10),
				},
				ElasticsearchVersion: "OpenSearch_1.3",
				InstanceType:         "r7g.large.search",
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "search-medium",
					},
					InstanceSizeRank: 20,
					DataCount:        "2",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-large",
						Name: "search-large",
					},
					InstanceType:     "r7g.large.search",
					InstanceSizeRank: 30,
					DataCount:        "2",
				},
			},
			targetPlanID:   "plan-medium",
			expectedPlanID: "plan-large",
			expectedErrMsg: "downgrading",
			expectedStatus: http.StatusBadRequest,
		},
		"non-HA to HA accepted": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-medium",
					},
					Uuid: helpers.RandStr(10),
				},
				InstanceType: "r7g.medium.search",
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "search-medium",
					},
					InstanceSizeRank: 20,
					DataCount:        "2",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium-ha",
						Name: "search-medium-ha",
					},
					InstanceType:     "r7g.medium.search",
					InstanceSizeRank: 20,
					DataCount:        "4",
				},
			},
			targetPlanID:   "plan-medium-ha",
			expectedPlanID: "plan-medium-ha",
		},
		"HA to non-HA rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-large-ha",
					},
					Uuid: helpers.RandStr(10),
				},
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-large",
						Name: "search-large",
					},
					InstanceSizeRank: 30,
					DataCount:        "2",
					InstanceType:     "r7g.large.search",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-large-ha",
						Name: "search-large-ha",
					},
					InstanceType:     "r7g.large.search",
					InstanceSizeRank: 30,
					DataCount:        "4",
				},
			},
			targetPlanID:   "plan-large",
			expectedPlanID: "plan-large-ha",
			expectedErrMsg: "reduce the number of data nodes",
			expectedStatus: http.StatusBadRequest,
		},
		"HA upgrade to larger HA accepted": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-medium-ha",
					},
					Uuid: helpers.RandStr(10),
				},
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium-ha",
						Name: "search-medium-ha",
					},
					InstanceSizeRank: 20,
					DataCount:        "4",
					InstanceType:     "r7g.medium.search",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-large-ha",
						Name: "search-large-ha",
					},
					InstanceType:     "r7g.large.search",
					InstanceSizeRank: 30,
					DataCount:        "4",
				},
			},
			targetPlanID:   "plan-large-ha",
			expectedPlanID: "plan-large-ha",
		},
		"plan without a size rank rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-medium",
					},
					Uuid: helpers.RandStr(10),
				},
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "search-medium",
					},
					InstanceSizeRank: 20,
					DataCount:        "2",
					InstanceType:     "r7g.medium.search",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-unranked",
						Name: "search-large",
					},
					InstanceType: "r7g.large.search",
					DataCount:    "2",
				},
			},
			targetPlanID:   "plan-unranked",
			expectedPlanID: "plan-medium",
			expectedErrMsg: "unable to determine plan sizes",
			expectedStatus: http.StatusBadRequest,
		},
		"single-node plan to multi-node plan rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-dev",
					},
					Uuid: helpers.RandStr(10),
				},
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-dev",
						Name: "es-dev",
					},
					InstanceSizeRank: 10,
					DataCount:        "1",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "search-medium",
					},
					InstanceSizeRank: 20,
					InstanceType:     "r7g.medium.search",
					DataCount:        "2",
				},
			},
			targetPlanID:   "plan-medium",
			expectedPlanID: "plan-dev",
			expectedErrMsg: "single-node plan to a multi-node plan",
			expectedStatus: http.StatusBadRequest,
		},
		"multi-node plan to single-node plan rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-medium",
					},
					Uuid: helpers.RandStr(10),
				},
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-dev",
						Name: "es-dev",
					},
					InstanceSizeRank: 10,
					DataCount:        "1",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "search-medium",
					},
					InstanceSizeRank: 20,
					InstanceType:     "r7g.medium.search",
					DataCount:        "2",
				},
			},
			targetPlanID:   "plan-dev",
			expectedPlanID: "plan-medium",
			expectedErrMsg: "reduce the number of data nodes",
			expectedStatus: http.StatusBadRequest,
		},
		"plan change with version upgrade rejected": {
			existingInstance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
						PlanID:    "plan-medium",
					},
					Uuid: helpers.RandStr(10),
				},
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			plans: []catalog.ElasticsearchPlan{
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-medium",
						Name: "es-medium",
					},
					InstanceSizeRank: 20,
					DataCount:        "2",
				},
				{
					ServicePlan: domain.ServicePlan{
						ID:   "plan-large",
						Name: "search-large",
					},
					InstanceSizeRank: 30,
					DataCount:        "2",
				},
			},
			targetPlanID:   "plan-large",
			expectedPlanID: "plan-medium",
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			expectedErrMsg: "plan change cannot be combined with an engine version upgrade",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			brokerDb, err := testDBInit()
			if err != nil {
				t.Fatal(err)
			}

			if err := brokerDb.Create(&base.Instance{Uuid: test.existingInstance.Uuid, Request: test.existingInstance.Request}).Error; err != nil {
				t.Fatal((err))
			}
			if err := brokerDb.Create(test.existingInstance).Error; err != nil {
				t.Fatal(err)
			}

			rawParams, _ := json.Marshal(test.options)
			updateDetails := domain.UpdateDetails{
				PlanID:        test.targetPlanID,
				RawParameters: rawParams,
			}

			broker := &elasticsearchBroker{
				brokerDB: brokerDb,
				catalog: &catalog.Catalog{
					ElasticsearchService: catalog.ElasticsearchService{
						ElasticsearchPlans: test.plans,
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

			err = broker.ModifyInstance(test.existingInstance.Uuid, updateDetails)

			if err != nil && test.expectedErrMsg == "" {
				t.Fatalf("unexpected error: %s", err)
			}

			if test.expectedErrMsg != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", test.expectedErrMsg)
				}
				if !strings.Contains(err.Error(), test.expectedErrMsg) {
					t.Fatalf("expected error containing %q, got %q", test.expectedErrMsg, err.Error())
				}
				if test.expectedStatus != 0 {
					failure, ok := err.(*apiresponses.FailureResponse)
					if !ok {
						t.Fatalf("expected *apiresponses.FailureResponse, got %T", err)
					}
					if got := failure.ValidatedStatusCode(nil); got != test.expectedStatus {
						t.Fatalf("expected HTTP status %d, got %d", test.expectedStatus, got)
					}
				}
			}

			persisted := ElasticsearchInstance{}
			if err := brokerDb.Where("uuid = ?", test.existingInstance.Uuid).First(&persisted).Error; err != nil {
				t.Fatalf("reloading instance: %s", err)
			}
			if persisted.PlanID != test.expectedPlanID {
				t.Fatalf("expected plan %q, got %q", test.expectedPlanID, persisted.PlanID)
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
