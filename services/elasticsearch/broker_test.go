package elasticsearch

import (
	"net/http"
	"testing"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/mocks"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
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

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	// Automigrate!
	db.AutoMigrate(&ElasticsearchInstance{}, &base.Instance{}, &jobs.AsyncJobMsg{})
	return db, err
}

func TestCreateInstance(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		planID               string
		instance             *ElasticsearchInstance
		expectedResponseCode int
		catalog              *catalog.Catalog
		createRequest        request.Request
		elasticsearchBroker  *elasticsearchBroker
	}{
		"success": {
			catalog: &catalog.Catalog{
				ElasticsearchService: catalog.ElasticsearchService{
					Plans: []catalog.ElasticsearchPlan{
						{
							Plan: catalog.Plan{
								ID: "123",
							},
						},
					},
				},
			},
			planID: "123",
			instance: &ElasticsearchInstance{
				Instance: base.Instance{
					Uuid: helpers.RandStr(10),
				},
			},
			createRequest: request.Request{
				PlanID: "123",
			},
			elasticsearchBroker: &elasticsearchBroker{
				settings: &config.Settings{
					EncryptionKey: helpers.RandStr(32),
					Environment:   "test", // use the mock adapter
				},
				tagManager: &mocks.MockTagGenerator{},
				brokerDB:   brokerDB,
				adapter:    &mockElasticsearchAdapter{},
			},
			expectedResponseCode: http.StatusAccepted,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			response := test.elasticsearchBroker.CreateInstance(test.catalog, test.instance.Uuid, test.createRequest)

			if response.GetStatusCode() != test.expectedResponseCode {
				t.Errorf("expected: %d, got: %d", test.expectedResponseCode, response.GetStatusCode())
			}
		})
	}
}
