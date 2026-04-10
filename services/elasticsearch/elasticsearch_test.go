package elasticsearch

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cloud-gov/aws-broker/config"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/go-test/deep"
)

type mockEsApiClient struct {
	getSnapshotStatusCalls     int
	getSnapshotStatusResponses []string
}

func (m *mockEsApiClient) CreateSnapshotRepo(repositoryName string, bucketName string, path string, region string, roleArn string) (string, error) {
	return "", nil
}

func (m *mockEsApiClient) CreateSnapshot(repositoryName string, snapshotName string) (string, error) {
	return "", nil
}

func (m *mockEsApiClient) GetSnapshotStatus(repositoryName string, snapshotName string) (string, error) {
	resp := m.getSnapshotStatusResponses[m.getSnapshotStatusCalls]
	m.getSnapshotStatusCalls += 1
	return resp, nil
}

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

func TestPollForSnapshotCreation(t *testing.T) {
	testCases := map[string]struct {
		esApiClient              EsApiClient
		esAdapter                *dedicatedElasticsearchAdapter
		expectedGetSnapshotCalls int
		expectErr                bool
	}{
		"success": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusResponses: []string{"SUCCESS"},
			},
			esAdapter: &dedicatedElasticsearchAdapter{
				settings: config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 1,
				},
			},
			expectedGetSnapshotCalls: 1,
		},
		"success with retries": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusResponses: []string{"IN PROGRESS", "IN PROGRESS", "SUCCESS"},
			},
			esAdapter: &dedicatedElasticsearchAdapter{
				settings: config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 3,
				},
			},
			expectedGetSnapshotCalls: 3,
		},
		"gives up after maximum retries": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusResponses: []string{"IN PROGRESS", "IN PROGRESS", "IN PROGRESS"},
			},
			esAdapter: &dedicatedElasticsearchAdapter{
				settings: config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 3,
				},
			},
			expectedGetSnapshotCalls: 3,
			expectErr:                true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.esAdapter.pollForSnapshotCreation(test.esApiClient, "foobar")
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if test.expectErr && err == nil {
				t.Fatal("expected error")
			}
			if mockEsApiClient, ok := test.esApiClient.(*mockEsApiClient); ok {
				if mockEsApiClient.getSnapshotStatusCalls != test.expectedGetSnapshotCalls {
					t.Fatalf("expected %d GetSnapshotStatus calls, got %d", test.expectedGetSnapshotCalls, mockEsApiClient.getSnapshotStatusCalls)
				}
			}
		})
	}
}
