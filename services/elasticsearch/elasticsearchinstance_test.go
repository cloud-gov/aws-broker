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
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
		},
		"version did upgrade": {
			i: &ElasticsearchInstance{
				TargetElasticsearchVersion: "version2",
				MasterEnabled:              false,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EngineVersion: aws.String("version2"),
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
			expectOk: true,
		},
		"version did not upgrade": {
			i: &ElasticsearchInstance{
				TargetElasticsearchVersion: "version2",
				MasterEnabled:              false,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EngineVersion: aws.String("version1"),
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
		},
		"manager instance type did upgrade": {
			i: &ElasticsearchInstance{
				MasterEnabled:      true,
				MasterInstanceType: string(opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch),
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterType:    opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch,
					DedicatedMasterEnabled: aws.Bool(true),
				},
			},
			expectOk: true,
		},
		"manager instance type did not upgrade": {
			i: &ElasticsearchInstance{
				MasterEnabled:      true,
				MasterInstanceType: string(opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch),
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterType:    opensearchTypes.OpenSearchPartitionInstanceTypeT3NanoSearch,
					DedicatedMasterEnabled: aws.Bool(true),
				},
			},
		},
		"volume size did update": {
			i: &ElasticsearchInstance{
				VolumeSize: 30,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EBSOptions: &opensearchTypes.EBSOptions{
					VolumeSize: aws.Int32(30),
				},
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
			expectOk: true,
		},
		"volume size did not update": {
			i: &ElasticsearchInstance{
				VolumeSize: 30,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EBSOptions: &opensearchTypes.EBSOptions{
					VolumeSize: aws.Int32(20),
				},
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
		},
		"manager node count did update": {
			i: &ElasticsearchInstance{
				MasterCount:   2,
				MasterEnabled: true,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterCount:   aws.Int32(2),
					DedicatedMasterEnabled: aws.Bool(true),
				},
			},
			expectOk: true,
		},
		"manager node count did not update": {
			i: &ElasticsearchInstance{
				MasterCount:   3,
				MasterEnabled: true,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					DedicatedMasterCount:   aws.Int32(2),
					DedicatedMasterEnabled: aws.Bool(true),
				},
			},
		},
		"data node count did update": {
			i: &ElasticsearchInstance{
				DataCount: 2,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceCount:          aws.Int32(2),
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
			expectOk: true,
		},
		"data node count did not update": {
			i: &ElasticsearchInstance{
				DataCount: 3,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceCount:          aws.Int32(2),
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
		},
		"data instance type did update": {
			i: &ElasticsearchInstance{
				InstanceType: string(opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch),
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceType:           opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch,
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
			expectOk: true,
		},
		"data instance type did not update": {
			i: &ElasticsearchInstance{
				InstanceType: string(opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch),
			},
			domainStatus: &opensearchTypes.DomainStatus{
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceType:           opensearchTypes.OpenSearchPartitionInstanceTypeM5LargeSearch,
					DedicatedMasterEnabled: aws.Bool(false),
				},
			},
		},
		"some updates succeeded but others did not": {
			i: &ElasticsearchInstance{
				TargetElasticsearchVersion: "version2",
				InstanceType:               string(opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch),
				MasterEnabled:              true,
				MasterInstanceType:         string(opensearchTypes.OpenSearchPartitionInstanceTypeC5LargeSearch),
				VolumeSize:                 30,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EngineVersion: aws.String("version2"),
				EBSOptions: &opensearchTypes.EBSOptions{
					VolumeSize: aws.Int32(20),
				},
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceType:           opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch,
					DedicatedMasterEnabled: aws.Bool(true),
					DedicatedMasterType:    opensearchTypes.OpenSearchPartitionInstanceTypeC5LargeSearch,
				},
			},
		},
		"all updates succeeded": {
			i: &ElasticsearchInstance{
				TargetElasticsearchVersion: "version2",
				InstanceType:               string(opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch),
				MasterEnabled:              true,
				MasterInstanceType:         string(opensearchTypes.OpenSearchPartitionInstanceTypeC5LargeSearch),
				MasterCount:                2,
				VolumeSize:                 30,
				DataCount:                  3,
			},
			domainStatus: &opensearchTypes.DomainStatus{
				EngineVersion: aws.String("version2"),
				EBSOptions: &opensearchTypes.EBSOptions{
					VolumeSize: aws.Int32(30),
				},
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceType:           opensearchTypes.OpenSearchPartitionInstanceTypeT3LargeSearch,
					InstanceCount:          aws.Int32(3),
					DedicatedMasterEnabled: aws.Bool(true),
					DedicatedMasterCount:   aws.Int32(2),
					DedicatedMasterType:    opensearchTypes.OpenSearchPartitionInstanceTypeC5LargeSearch,
				},
			},
			expectOk: true,
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
