package elasticsearch

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/go-test/deep"
)

func TestIsInvalidTypeException(t *testing.T) {
	isInvalidType := isInvalidTypeException(&opensearchTypes.InvalidTypeException{})
	if !isInvalidType {
		t.Fatal("expected isInvalidTypeException() to return true")
	}
}

func TestPrepareCreateDomainInput(t *testing.T) {
	testCases := map[string]struct {
		esInstance     *ElasticsearchInstance
		accessPolicy   string
		expectedParams *opensearch.CreateDomainInput
	}{
		"data count of 1": {
			esInstance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				DataCount:                  1,
				SubnetID2AZ2:               "az-2",
				SecGroup:                   "group-1",
				EncryptAtRest:              false,
				VolumeSize:                 10,
				VolumeType:                 "gp3",
				InstanceType:               "m5.2xlarge.search",
				NodeToNodeEncryption:       true,
				AutomatedSnapshotStartHour: 0,
				Tags: map[string]string{
					"foo": "bar",
				},
			},
			accessPolicy: "fake-access-policy",
			expectedParams: &opensearch.CreateDomainInput{
				DomainName:     aws.String("test-domain"),
				AccessPolicies: aws.String("fake-access-policy"),
				VPCOptions: &opensearchTypes.VPCOptions{
					SubnetIds:        []string{"az-2"},
					SecurityGroupIds: []string{"group-1"},
				},
				DomainEndpointOptions: &opensearchTypes.DomainEndpointOptions{
					EnforceHTTPS: aws.Bool(true),
				},
				EBSOptions: &opensearchTypes.EBSOptions{
					EBSEnabled: aws.Bool(true),
					VolumeSize: aws.Int32(int32(10)),
					VolumeType: opensearchTypes.VolumeTypeGp3,
				},
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceType:  opensearchTypes.OpenSearchPartitionInstanceTypeM52xlargeSearch,
					InstanceCount: aws.Int32(int32(1)),
				},
				SnapshotOptions: &opensearchTypes.SnapshotOptions{
					AutomatedSnapshotStartHour: aws.Int32(int32(0)),
				},
				NodeToNodeEncryptionOptions: &opensearchTypes.NodeToNodeEncryptionOptions{
					Enabled: aws.Bool(true),
				},
				EncryptionAtRestOptions: &opensearchTypes.EncryptionAtRestOptions{
					Enabled: aws.Bool(false),
				},
				TagList: []opensearchTypes.Tag{
					{
						Key:   aws.String("foo"),
						Value: aws.String("bar"),
					},
				},
			},
		},
		"data count is greater than 1": {
			esInstance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				DataCount:                  2,
				SubnetID3AZ1:               "az-3",
				SubnetID4AZ2:               "az-4",
				SecGroup:                   "group-1",
				EncryptAtRest:              false,
				VolumeSize:                 10,
				VolumeType:                 "gp3",
				InstanceType:               "m5.2xlarge.search",
				NodeToNodeEncryption:       true,
				AutomatedSnapshotStartHour: 0,
			},
			accessPolicy: "fake-access-policy",
			expectedParams: &opensearch.CreateDomainInput{
				DomainName:     aws.String("test-domain"),
				AccessPolicies: aws.String("fake-access-policy"),
				VPCOptions: &opensearchTypes.VPCOptions{
					SubnetIds:        []string{"az-3", "az-4"},
					SecurityGroupIds: []string{"group-1"},
				},
				DomainEndpointOptions: &opensearchTypes.DomainEndpointOptions{
					EnforceHTTPS: aws.Bool(true),
				},
				EBSOptions: &opensearchTypes.EBSOptions{
					EBSEnabled: aws.Bool(true),
					VolumeSize: aws.Int32(int32(10)),
					VolumeType: opensearchTypes.VolumeTypeGp3,
				},
				ClusterConfig: &opensearchTypes.ClusterConfig{
					InstanceType:         opensearchTypes.OpenSearchPartitionInstanceTypeM52xlargeSearch,
					InstanceCount:        aws.Int32(int32(2)),
					ZoneAwarenessEnabled: aws.Bool(true),
					ZoneAwarenessConfig: &opensearchTypes.ZoneAwarenessConfig{
						AvailabilityZoneCount: aws.Int32(int32(2)),
					},
				},
				SnapshotOptions: &opensearchTypes.SnapshotOptions{
					AutomatedSnapshotStartHour: aws.Int32(int32(0)),
				},
				NodeToNodeEncryptionOptions: &opensearchTypes.NodeToNodeEncryptionOptions{
					Enabled: aws.Bool(true),
				},
				EncryptionAtRestOptions: &opensearchTypes.EncryptionAtRestOptions{
					Enabled: aws.Bool(false),
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := prepareCreateDomainInput(
				test.esInstance,
				test.accessPolicy,
			)
			if err != nil {
				t.Fatal(err)
			}
			if diff := deep.Equal(params, test.expectedParams); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestPrepareUpdateDomainConfigInput(t *testing.T) {
	testCases := map[string]struct {
		esInstance     *ElasticsearchInstance
		expectedParams *opensearch.UpdateDomainConfigInput
	}{
		"no ebs options": {
			esInstance: &ElasticsearchInstance{
				Domain: "fake-domain",
			},
			expectedParams: &opensearch.UpdateDomainConfigInput{
				DomainName:      aws.String("fake-domain"),
				AdvancedOptions: map[string]string{},
			},
		},
		"update volume type": {
			esInstance: &ElasticsearchInstance{
				Domain:     "fake-domain",
				VolumeType: "gp3",
				VolumeSize: 15,
			},
			expectedParams: &opensearch.UpdateDomainConfigInput{
				DomainName:      aws.String("fake-domain"),
				AdvancedOptions: map[string]string{},
				EBSOptions: &opensearchTypes.EBSOptions{
					EBSEnabled: aws.Bool(true),
					VolumeType: opensearchTypes.VolumeTypeGp3,
					VolumeSize: aws.Int32(15),
				},
			},
		},
		"set field cache data size": {
			esInstance: &ElasticsearchInstance{
				Domain:                    "fake-domain",
				IndicesFieldDataCacheSize: "1000",
			},
			expectedParams: &opensearch.UpdateDomainConfigInput{
				DomainName: aws.String("fake-domain"),
				AdvancedOptions: map[string]string{
					"indices.fielddata.cache.size": "1000",
				},
			},
		},
		"set max clause count": {
			esInstance: &ElasticsearchInstance{
				Domain:                         "fake-domain",
				IndicesQueryBoolMaxClauseCount: "5000",
			},
			expectedParams: &opensearch.UpdateDomainConfigInput{
				DomainName: aws.String("fake-domain"),
				AdvancedOptions: map[string]string{
					"indices.query.bool.max_clause_count": "5000",
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := prepareUpdateDomainConfigInput(test.esInstance)
			if err != nil {
				t.Fatal(err)
			}
			if diff := deep.Equal(params, test.expectedParams); diff != nil {
				t.Error(diff)
			}
		})
	}
}
