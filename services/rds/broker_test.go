package rds

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	responseHelpers "github.com/cloud-gov/aws-broker/helpers/response"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	"github.com/cloud-gov/aws-broker/services/redis"
	"github.com/cloud-gov/aws-broker/taskqueue"
	"github.com/jinzhu/gorm"

	brokertags "github.com/cloud-gov/go-broker-tags"
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
	db.AutoMigrate(&RDSInstance{}, &redis.RedisInstance{}, &elasticsearch.ElasticsearchInstance{}, &base.Instance{}, &taskqueue.AsyncJobMsg{}) // Add all your models here to help setup the database tables
	return db, err
}

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
		queueManager         taskqueue.QueueManager
		tagManager           brokertags.TagManager
		settings             *config.Settings
		catalog              *catalog.Catalog
		createRequest        request.Request
	}{
		"success": {
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
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
			queueManager: &mockQueueManager{},
			tagManager:   &mocks.MockTagGenerator{},
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
				taskqueue:  test.queueManager,
			}

			response := broker.CreateInstance(test.catalog, test.dbInstance.Uuid, test.createRequest)

			if response.GetStatusCode() != test.expectedResponseCode {
				t.Errorf("expected: %d, got: %d", test.expectedResponseCode, response.GetStatusCode())
			}
		})
	}
}

func TestLastOperation(t *testing.T) {
	testCases := map[string]struct {
		planID        string
		dbInstance    *RDSInstance
		expectedState string
		tagManager    brokertags.TagManager
		settings      *config.Settings
		catalog       *catalog.Catalog
		operation     string
		asyncJobMsg   *taskqueue.AsyncJobMsg
	}{
		"create without replica": {
			operation: base.CreateOp.String(),
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
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
			expectedState: "succeeded",
		},
		"create with replica": {
			operation: base.CreateOp.String(),
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
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
				ReplicaDatabase: "replica",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			expectedState: "succeeded",
			asyncJobMsg: &taskqueue.AsyncJobMsg{
				JobType: base.CreateOp,
				JobState: taskqueue.AsyncJobState{
					Message: "completed",
					State:   base.InstanceReady,
				},
			},
		},
		"default": {
			operation: base.ModifyOp.String(),
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
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
			expectedState: "succeeded",
		},
		"modify without replica": {
			operation: base.ModifyOp.String(),
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
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
			expectedState: "succeeded",
		},
		"modify with replica": {
			operation: base.ModifyOp.String(),
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
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
				ReplicaDatabase: "replica",
			},
			tagManager: &mocks.MockTagGenerator{},
			settings: &config.Settings{
				EncryptionKey: helpers.RandStr(32),
				Environment:   "test", // use the mock adapter
			},
			expectedState: "succeeded",
			asyncJobMsg: &taskqueue.AsyncJobMsg{
				JobType: base.ModifyOp,
				JobState: taskqueue.AsyncJobState{
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

			response := broker.LastOperation(test.catalog, test.dbInstance.Uuid, base.Instance{
				Request: request.Request{
					PlanID: test.planID,
				},
			}, test.operation)

			lastOperationResponse, ok := response.(*responseHelpers.LastOperationResponse)

			if !ok {
				t.Fatal(lastOperationResponse)
			}

			if lastOperationResponse.State != test.expectedState {
				t.Errorf("expected: %s, got: %s", test.expectedState, lastOperationResponse.State)
			}
		})
	}
}
