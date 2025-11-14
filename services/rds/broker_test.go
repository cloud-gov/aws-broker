package rds

import (
	"net/http"
	"reflect"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/go-test/deep"

	brokertags "github.com/cloud-gov/go-broker-tags"
)

func TestValidate(t *testing.T) {
	testCases := map[string]struct {
		options     Options
		settings    *config.Settings
		expectedErr bool
	}{
		"invalid binary log format": {
			options: Options{
				BinaryLogFormat: "foo",
			},
			settings:    &config.Settings{},
			expectedErr: true,
		},
		"MIXED binary log format": {
			options: Options{
				BinaryLogFormat: "MIXED",
			},
			settings:    &config.Settings{},
			expectedErr: false,
		},
		"accepted storage type": {
			options: Options{
				StorageType: "gp3",
			},
			settings:    &config.Settings{},
			expectedErr: false,
		},
		"invalid storage type": {
			options: Options{
				StorageType: "io1",
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

func TestParseModifyOptionsFromRequest(t *testing.T) {
	testCases := map[string]struct {
		broker          *rdsBroker
		modifyRequest   request.Request
		expectedOptions Options
		expectErr       bool
	}{
		"enable PG cron not specified": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(``),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"enable PG cron true": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "enable_pg_cron": true }`),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				EnablePgCron:       aws.Bool(true),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"enable PG cron false": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "enable_pg_cron": false }`),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				EnablePgCron:       aws.Bool(false),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"rotate creds true": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "rotate_credentials": true }`),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				RotateCredentials:  aws.Bool(true),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"rotate creds false": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{ "rotate_credentials": false }`),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				RotateCredentials:  aws.Bool(false),
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"rotate creds not specified": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{}`),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
		},
		"backup retention period less than minimum is rejected": {
			broker: &rdsBroker{
				settings: &config.Settings{
					MinBackupRetention: 14,
				},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{"backup_retention_period": 0}`),
			},
			expectedOptions: Options{
				AllocatedStorage:      0,
				EnableFunctions:       false,
				PubliclyAccessible:    false,
				Version:               "",
				BinaryLogFormat:       "",
				BackupRetentionPeriod: aws.Int64(0),
			},
			expectErr: true,
		},
		"allocated storage exceeding maxmimum is rejected": {
			broker: &rdsBroker{
				settings: &config.Settings{
					MaxAllocatedStorage: 100,
				},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{"storage": 150}`),
			},
			expectedOptions: Options{
				AllocatedStorage:   150,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
			expectErr: true,
		},
		"throws error on invalid JSON": {
			broker: &rdsBroker{
				settings: &config.Settings{},
			},
			modifyRequest: request.Request{
				RawParameters: []byte(`{"foo": }`),
			},
			expectedOptions: Options{
				AllocatedStorage:   0,
				EnableFunctions:    false,
				PubliclyAccessible: false,
				Version:            "",
				BinaryLogFormat:    "",
			},
			expectErr: true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			options, err := test.broker.parseModifyOptionsFromRequest(test.modifyRequest)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !reflect.DeepEqual(test.expectedOptions, options) {
				t.Errorf("expected: %+v, got %+v", test.expectedOptions, options)
			}
		})
	}
}

func TestCreateInstanceSuccess(t *testing.T) {
	testCases := map[string]struct {
		planID               string
		dbInstance           *RDSInstance
		expectedResponseCode int
		tagManager           brokertags.TagManager
		settings             *config.Settings
		catalog              *catalog.Catalog
		createRequest        request.Request
	}{
		"success": {
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
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			createRequest: request.Request{
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

			broker := &rdsBroker{
				brokerDB:   brokerDB,
				settings:   test.settings,
				tagManager: test.tagManager,
				dbAdapter:  &mockDBAdapter{},
			}

			response := broker.CreateInstance(test.catalog, test.dbInstance.Uuid, test.createRequest)

			if response.GetStatusCode() != test.expectedResponseCode {
				t.Errorf("expected: %d, got: %d", test.expectedResponseCode, response.GetStatusCode())
			}
		})
	}
}

func TestModify(t *testing.T) {
	testCases := map[string]struct {
		dbInstance           *RDSInstance
		expectedResponseCode int
		tagManager           brokertags.TagManager
		settings             *config.Settings
		catalog              *catalog.Catalog
		modifyRequest        request.Request
		expectedDbInstance   *RDSInstance
		dbAdapter            dbAdapter
	}{
		"success": {
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID:             "123",
								PlanUpdateable: true,
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
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "456",
					},
				},
			},
			expectedDbInstance: &RDSInstance{
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
			modifyRequest: request.Request{
				PlanID: "123",
			},
			expectedResponseCode: http.StatusAccepted,
		},
		"success with replica": {
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					RDSPlans: []catalog.RDSPlan{
						{
							ServicePlan: domain.ServicePlan{
								ID:             "123",
								PlanUpdateable: true,
							},
							Redundant:   true,
							ReadReplica: true,
						},
						{
							ServicePlan: domain.ServicePlan{
								ID: "456",
							},
						},
					},
				},
			},
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "456",
					},
				},
			},
			expectedDbInstance: &RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						ServiceID: "service-1",
						PlanID:    "123",
					},
					State: base.InstanceInProgress,
				},
				ReplicaDatabase: "-replica",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			modifyRequest: request.Request{
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

			broker := &rdsBroker{
				brokerDB:   brokerDB,
				settings:   test.settings,
				tagManager: test.tagManager,
				dbAdapter: &mockDBAdapter{
					db: brokerDB,
				},
			}

			err = brokerDB.Create(test.dbInstance).Error
			if err != nil {
				t.Fatal(err)
			}

			response := broker.ModifyInstance(test.catalog, test.dbInstance.Uuid, test.modifyRequest, base.Instance{})

			if response.GetStatusCode() != test.expectedResponseCode {
				t.Errorf("expected: %d, got: %d", test.expectedResponseCode, response.GetStatusCode())
			}

			updatedInstance := &RDSInstance{}
			err = broker.brokerDB.First(updatedInstance, test.dbInstance.Uuid).Error
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
		dbInstance    *RDSInstance
		expectedState base.InstanceState
		tagManager    brokertags.TagManager
		settings      *config.Settings
		catalog       *catalog.Catalog
		asyncJobMsg   *jobs.AsyncJobMsg
		pollDetails   domain.PollDetails
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
			dbInstance: &RDSInstance{
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
				JobType: base.CreateOp,
				JobState: jobs.AsyncJobState{
					Message: "completed",
					State:   base.InstanceReady,
				},
			},
			expectedState: base.InstanceReady,
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
			dbInstance: &RDSInstance{
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
			dbInstance: &RDSInstance{
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

			broker := &rdsBroker{
				brokerDB:   brokerDB,
				settings:   test.settings,
				tagManager: test.tagManager,
				dbAdapter:  &mockDBAdapter{},
			}

			err = brokerDB.Create(test.dbInstance).Error
			if err != nil {
				t.Fatal(err)
			}

			if test.asyncJobMsg != nil {
				test.asyncJobMsg.BrokerId = test.dbInstance.ServiceID
				test.asyncJobMsg.InstanceId = test.dbInstance.Uuid
				err := brokerDB.Create(test.asyncJobMsg).Error
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
