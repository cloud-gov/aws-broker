package elasticsearch

import (
	"context"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/testutil"

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

func domainStatus(processing bool) *opensearch.DescribeDomainOutput {
	return &opensearch.DescribeDomainOutput{
		DomainStatus: &opensearchTypes.DomainStatus{
			ARN:           aws.String("test-arn"),
			ClusterConfig: &opensearchTypes.ClusterConfig{},
			DomainId:      aws.String("test-id"),
			DomainName:    aws.String(("test-domain")),
			Created:       aws.Bool(true),
			Processing:    aws.Bool(processing),
		},
	}
}

func TestCheckElasticsearchStatus(t *testing.T) {
	testCases := map[string]struct {
		instance                         *ElasticsearchInstance
		describeDomainResults            []*opensearch.DescribeDomainOutput
		upgradeStatus                    opensearchTypes.UpgradeStatus
		upgradeStatusErr                 error
		expectedState                    base.InstanceState
		expectedVersionUpgradeInProgress bool
		expectedESVersion                string
		expectedTargetESVersion          string
	}{
		"Processing true keeps status as in progress": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
				VersionUpgradeInProgress:   true,
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(true)},
			expectedState:                    base.InstanceInProgress,
			expectedVersionUpgradeInProgress: true,
			expectedESVersion:                "OpenSearch_1.3",
			expectedTargetESVersion:          "OpenSearch_2.3",
		},
		"upgrade succeeded promotes version and clears target": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
				VersionUpgradeInProgress:   true,
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false)},
			upgradeStatus:                    opensearchTypes.UpgradeStatusSucceeded,
			expectedState:                    base.InstanceReady,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_2.3",
			expectedTargetESVersion:          "",
		},
		"upgrade succeeded with issues promotes version and clears target": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
				VersionUpgradeInProgress:   true,
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false)},
			upgradeStatus:                    opensearchTypes.UpgradeStatusSucceededWithIssues,
			expectedState:                    base.InstanceReady,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_2.3",
			expectedTargetESVersion:          "",
		},
		"upgrade failed leaves version unchanged and clears target": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
				VersionUpgradeInProgress:   true,
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false)},
			upgradeStatus:                    opensearchTypes.UpgradeStatusFailed,
			expectedState:                    base.InstanceNotModified,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_1.3",
			expectedTargetESVersion:          "",
		},
		"upgrade still in progress keeps polling": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
				VersionUpgradeInProgress:   true,
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false)},
			upgradeStatus:                    opensearchTypes.UpgradeStatusInProgress,
			expectedState:                    base.InstanceInProgress,
			expectedVersionUpgradeInProgress: true,
			expectedESVersion:                "OpenSearch_1.3",
			expectedTargetESVersion:          "OpenSearch_2.3",
		},
		"non-upgrade Processing false returns ready without calling": {
			instance: &ElasticsearchInstance{
				Instance:                 base.Instance{State: base.InstanceInProgress},
				Domain:                   "test-domain",
				ElasticsearchVersion:     "OpenSearch_1.3",
				VersionUpgradeInProgress: false,
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false)},
			expectedState:                    base.InstanceReady,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_1.3",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			mock := &mockOpensearchClient{
				describeDomainResults: test.describeDomainResults,
				upgradeStatus:         test.upgradeStatus,
				upgradeStatusErr:      test.upgradeStatusErr,
			}
			adapter := &dedicatedElasticsearchAdapter{
				ctx:        context.Background(),
				opensearch: mock,
				logger:     slog.New(&testutil.MockLogHandler{}),
			}

			state, err := adapter.checkElasticsearchStatus(test.instance)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if state != test.expectedState {
				t.Errorf("expected state %v, got %v", test.expectedState, state)
			}
			if test.instance.VersionUpgradeInProgress != test.expectedVersionUpgradeInProgress {
				t.Errorf("expected VersionUpgradeInProgress=%v, got %v", test.expectedVersionUpgradeInProgress, test.instance.VersionUpgradeInProgress)
			}
			if test.expectedESVersion != "" && test.instance.ElasticsearchVersion != test.expectedESVersion {
				t.Errorf("expected ElasticsearchVersion=%q, got %q", test.expectedESVersion, test.instance.ElasticsearchVersion)
			}
			if test.instance.TargetElasticsearchVersion != test.expectedTargetESVersion {
				t.Errorf("expected TargetElasticsearchVersion=%q, got %q", test.expectedTargetESVersion, test.instance.TargetElasticsearchVersion)
			}
		})
	}
}
