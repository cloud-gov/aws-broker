package elasticsearch

import (
	"testing"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/go-test/deep"
)

func TestInitInstanceTags(t *testing.T) {
	plan := catalog.ElasticsearchPlan{
		Tags: map[string]string{
			"plan-tag-1": "foo",
		},
	}
	tags := map[string]string{
		"tag-1": "bar",
	}

	instance := &ElasticsearchInstance{}
	instance.init(
		"uuid-1",
		"org-1",
		"space-1",
		"service-1",
		plan,
		ElasticsearchOptions{},
		&config.Settings{
			EncryptionKey: helpers.RandStr(16),
		},
		tags,
	)

	expectedTags := map[string]string{
		"plan-tag-1": "foo",
		"tag-1":      "bar",
	}

	if diff := deep.Equal(instance.Tags, expectedTags); diff != nil {
		t.Error(diff)
	}
}

func TestUpdateInstance(t *testing.T) {
	testCases := map[string]struct {
		options          ElasticsearchOptions
		existingInstance *ElasticsearchInstance
		expectedInstance *ElasticsearchInstance
		expectErr        bool
	}{
		"gp3 upgrade succeeds": {
			options: ElasticsearchOptions{
				VolumeType: "gp3",
			},
			existingInstance: &ElasticsearchInstance{
				VolumeType: "gp2",
			},
			expectedInstance: &ElasticsearchInstance{
				VolumeType: "gp3",
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.existingInstance.update(test.options)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if diff := deep.Equal(test.existingInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}
