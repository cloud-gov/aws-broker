package elasticsearch

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
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
		existingVersion          string
		versionUpgradeInProgress bool
		expectedErrMsg           string
		expectedStatus           int
		// plan change fields
		currentPlanName     string
		currentInstanceType string
		currentDataCount    string
		targetPlanID        string
		targetPlanName      string
		targetInstanceType  string
		targetDataCount     string
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
		"version with log publishing rejected": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
				LogPublishing:        ElasticsearchLogOptions{ErrorLogs: aws.Bool(true)},
			},
			existingVersion: "OpenSearch_1.3",
			expectedErrMsg:  "engine version upgrade cannot be combined with other configuration options",
		},
		"non-HA upgrade to larger plan accepted": {
			currentPlanName:     "es-medium-memory-optimized",
			currentInstanceType: "r8g.medium.search",
			currentDataCount:    "2",
			targetPlanID:        "plan-large",
			targetPlanName:      "es-large-memory-optimized",
			targetInstanceType:  "r8g.large.search",
			targetDataCount:     "2",
		},
		"non-HA downgrade rejected": {
			currentPlanName:     "es-large-memory-optimized",
			currentInstanceType: "r8g.large.search",
			currentDataCount:    "2",
			targetPlanID:        "plan-medium",
			targetPlanName:      "es-medium-memory-optimized",
			targetInstanceType:  "r8g.medium.search",
			targetDataCount:     "2",
			expectedErrMsg:      "downgrading",
		},
		"non-HA to HA rejected": {
			currentPlanName:     "es-medium-memory-optimized",
			currentInstanceType: "r8g.medium.search",
			currentDataCount:    "2",
			targetPlanID:        "plan-medium-ha",
			targetPlanName:      "es-medium-memory-optimized-ha",
			targetInstanceType:  "r8g.medium.search",
			targetDataCount:     "4",
			expectedErrMsg:      "highly-available",
		},
		"HA to non-HA rejected": {
			currentPlanName:     "es-large-memory-optimized-ha",
			currentInstanceType: "r8g.large.search",
			currentDataCount:    "4",
			targetPlanID:        "plan-large",
			targetPlanName:      "es-large-memory-optimized",
			targetInstanceType:  "r8g.large.search",
			targetDataCount:     "2",
			expectedErrMsg:      "highly-available",
		},
		"HA upgrade to larger HA accepted": {
			currentPlanName:     "es-medium-memory-optimized-ha",
			currentInstanceType: "r8g.medium.search",
			currentDataCount:    "4",
			targetPlanID:        "plan-large-ha",
			targetPlanName:      "es-large-memory-optimized-ha",
			targetInstanceType:  "r8g.large.search",
			targetDataCount:     "4",
		},
		"single-node plan to multi-node plan rejected": {
			currentPlanName:     "es-dev",
			currentInstanceType: "t3.small.search",
			currentDataCount:    "1",
			targetPlanID:        "plan-medium",
			targetPlanName:      "es-medium-memory-optimized",
			targetInstanceType:  "r8g.medium.search",
			targetDataCount:     "2",
			expectedErrMsg:      "single-node and multi-node",
			expectedStatus:      http.StatusBadRequest,
		},
		"multi-node plan to single-node plan rejected": {
			currentPlanName:     "es-medium-memory-optimized",
			currentInstanceType: "r8g.medium.search",
			currentDataCount:    "2",
			targetPlanID:        "plan-dev",
			targetPlanName:      "es-dev",
			targetInstanceType:  "t3.small.search",
			targetDataCount:     "1",
			expectedErrMsg:      "single-node and multi-node",
			expectedStatus:      http.StatusBadRequest,
		},
		"plan change with version upgrade rejected": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			existingVersion:     "OpenSearch_1.3",
			currentPlanName:     "es-medium-memory-optimized",
			currentInstanceType: "r8g.medium.search",
			currentDataCount:    "2",
			targetPlanID:        "plan-large",
			targetPlanName:      "es-large-memory-optimized",
			targetInstanceType:  "r8g.large.search",
			targetDataCount:     "2",
			expectedErrMsg:      "plan change cannot be combined with an engine version upgrade",
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
				InstanceType:         test.currentInstanceType,
			}
			if test.currentDataCount != "" {
				existingInstance.DataCount, _ = strconv.Atoi(test.currentDataCount)
			}
			if test.versionUpgradeInProgress {
				existingInstance.TargetElasticsearchVersion = "OpenSearch_2.3"
			}
			if err := brokerDb.Create(&base.Instance{Uuid: instanceId, Request: existingInstance.Request}).Error; err != nil {
				t.Fatal((err))
			}
			if err := brokerDb.Create(existingInstance).Error; err != nil {
				t.Fatal(err)
			}

			rawParams, _ := json.Marshal(test.options)
			targetPlanID := planId
			if test.targetPlanID != "" {
				targetPlanID = test.targetPlanID
			}
			updateDetails := domain.UpdateDetails{
				PlanID:        targetPlanID,
				RawParameters: rawParams,
			}

			plans := []catalog.ElasticsearchPlan{
				{
					ServicePlan:  domain.ServicePlan{ID: planId, Name: test.currentPlanName},
					InstanceType: test.currentInstanceType,
					DataCount:    test.currentDataCount,
				},
			}
			if test.targetPlanID != "" {
				plans = append(plans, catalog.ElasticsearchPlan{
					ServicePlan:  domain.ServicePlan{ID: test.targetPlanID, Name: test.targetPlanName},
					InstanceType: test.targetInstanceType,
					DataCount:    test.targetDataCount,
				})
			}

			broker := &elasticsearchBroker{
				brokerDB: brokerDb,
				catalog: &catalog.Catalog{
					ElasticsearchService: catalog.ElasticsearchService{
						ElasticsearchPlans: plans,
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
				if test.expectedStatus != 0 {
					failure, ok := err.(*apiresponses.FailureResponse)
					if !ok {
						t.Fatalf("expected *apiresponses.FailureResponse, got %T", err)
					}
					if got := failure.ValidatedStatusCode(nil); got != test.expectedStatus {
						t.Fatalf("expected HTTP status %d, got %d", test.expectedStatus, got)
					}
				}
				if test.targetPlanID != "" {
					persisted := ElasticsearchInstance{}
					if err := brokerDb.Where("uuid = ?", instanceId).First(&persisted).Error; err != nil {
						t.Fatalf("reloading instance: %s", err)
					}
					if persisted.PlanID != planId {
						t.Fatalf("expected instance to remain on plan %q after rejection, got %q", planId, persisted.PlanID)
					}
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
