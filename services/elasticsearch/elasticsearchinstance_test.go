package elasticsearch

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
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
	instance.init( //nolint:errcheck // test setup; init failure surfaces downstream in the test
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
		"version upgrade stages TargetElasticSearchVersion": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			existingInstance: &ElasticsearchInstance{
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			expectedInstance: &ElasticsearchInstance{
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
			},
		},
		"same version does not stage TargetElasticSearchVersion": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			existingInstance: &ElasticsearchInstance{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
			expectedInstance: &ElasticsearchInstance{
				ElasticsearchVersion: "OpenSearch_2.3",
			},
		},
		"empty version is a no-op": {
			options: ElasticsearchOptions{
				ElasticsearchVersion: "",
			},
			existingInstance: &ElasticsearchInstance{
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			expectedInstance: &ElasticsearchInstance{
				ElasticsearchVersion: "OpenSearch_1.3",
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

func TestUpgradesAreSuccessful(t *testing.T) {
	testCases := map[string]struct {
		i            *ElasticsearchInstance
		domainStatus *opensearchTypes.DomainStatus
		expectOk     bool
	}{
		"no upgrades": {
			i:        &ElasticsearchInstance{},
			expectOk: true,
		},
		"version did upgrade": {
			i: &ElasticsearchInstance{
				TargetElasticsearchVersion: "version2",
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EngineVersion: aws.String("version2"),
			},
			expectOk: true,
		},
		"version did not upgrade": {
			i: &ElasticsearchInstance{
				TargetElasticsearchVersion: "version2",
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EngineVersion: aws.String("version1"),
			},
		},
		"manager instance type did upgrade": {
			i: &ElasticsearchInstance{
				MasterInstanceType: string(opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch),
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterType: opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch,
				},
			},
			expectOk: true,
		},
		"manager instance type did not upgrade": {
			i: &ElasticsearchInstance{
				MasterInstanceType: string(opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch),
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterType: opensearchTypes.OpenSearchPartitionInstanceTypeT3NanoSearch,
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			ok := test.i.upgradesAreSuccessful(test.domainStatus)
			if ok != test.expectOk {
				t.Fatalf("expected: %t, got: %t", test.expectOk, ok)
			}
		})
	}
}
