package elasticsearch

import (
	"context"
	"errors"
	"log/slog"
	"strings"
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

func domainStatus(processing bool, upgradeProcessing bool, engineVersion string) *opensearch.DescribeDomainOutput {
	return &opensearch.DescribeDomainOutput{
		DomainStatus: &opensearchTypes.DomainStatus{
			ARN:               aws.String("test-arn"),
			ClusterConfig:     &opensearchTypes.ClusterConfig{},
			DomainId:          aws.String("test-id"),
			DomainName:        aws.String(("test-domain")),
			Created:           aws.Bool(true),
			Processing:        aws.Bool(processing),
			UpgradeProcessing: aws.Bool(upgradeProcessing),
			EngineVersion:     aws.String(engineVersion),
		},
	}
}

func TestCheckElasticsearchStatus(t *testing.T) {
	testCases := map[string]struct {
		instance                         *ElasticsearchInstance
		describeDomainResults            []*opensearch.DescribeDomainOutput
		expectedState                    base.InstanceState
		expectedVersionUpgradeInProgress bool
		expectedESVersion                string
		expectedTargetESVersion          string
	}{
		"UpgradeProcessing true keeps status as in progress": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false, true, "OpenSearch_1.3")},
			expectedState:                    base.InstanceInProgress,
			expectedVersionUpgradeInProgress: true,
			expectedESVersion:                "OpenSearch_1.3",
			expectedTargetESVersion:          "OpenSearch_2.3",
		},
		"upgrade done and EngineVersion matches target": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false, false, "OpenSearch_2.3")},
			expectedState:                    base.InstanceReady,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_2.3",
			expectedTargetESVersion:          "",
		},
		"upgrade done but old EngineVersion reports failed and leaves versions unchanged": {
			instance: &ElasticsearchInstance{
				Instance:                   base.Instance{State: base.InstanceInProgress},
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false, false, "OpenSearch_1.3")},
			expectedState:                    base.InstanceNotModified,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_1.3",
			expectedTargetESVersion:          "",
		},
		"Processing false and UpgradeProcessing false returns ready": {
			instance: &ElasticsearchInstance{
				Instance:             base.Instance{State: base.InstanceInProgress},
				Domain:               "test-domain",
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(false, false, "OpenSearch_1.3")},
			expectedState:                    base.InstanceReady,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_1.3",
			expectedTargetESVersion:          "",
		},
		"Processing true keeps status as in progress": {
			instance: &ElasticsearchInstance{
				Instance:             base.Instance{State: base.InstanceInProgress},
				Domain:               "test-domain",
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			describeDomainResults:            []*opensearch.DescribeDomainOutput{domainStatus(true, false, "OpenSearch_1.3")},
			expectedState:                    base.InstanceInProgress,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_1.3",
		},
		"nil Processing and UpgradeProcessing returns ready": {
			instance: &ElasticsearchInstance{
				Instance:             base.Instance{State: base.InstanceInProgress},
				Domain:               "test-domain",
				ElasticsearchVersion: "OpenSearch_1.3",
			},
			describeDomainResults: []*opensearch.DescribeDomainOutput{{
				DomainStatus: &opensearchTypes.DomainStatus{
					Created: aws.Bool(true),
				},
			}},
			expectedState:                    base.InstanceReady,
			expectedVersionUpgradeInProgress: false,
			expectedESVersion:                "OpenSearch_1.3",
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			mock := &mockOpensearchClient{
				describeDomainResults: test.describeDomainResults,
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
			if test.instance.versionUpgradeInProgress() != test.expectedVersionUpgradeInProgress {
				t.Errorf("expected versionUpgradeInProgress()=%v, got %v", test.expectedVersionUpgradeInProgress, test.instance.versionUpgradeInProgress())
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

func TestModifyElasticsearch(t *testing.T) {
	testCases := map[string]struct {
		instance            *ElasticsearchInstance
		upgradeDomainErr    error
		expectUpgradeCalled bool
		expectErr           bool
		expectedState       base.InstanceState
	}{
		"version upgrae calls UpgradeDomain": {
			instance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
			},
			expectUpgradeCalled: true,
			expectedState:       base.InstanceInProgress,
		},
		"UpgradeDomain error returns InstanceNotModified": {
			instance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				ElasticsearchVersion:       "OpenSearch_1.3",
				TargetElasticsearchVersion: "OpenSearch_2.3",
			},
			upgradeDomainErr:    errors.New("upgrade failed"),
			expectUpgradeCalled: true,
			expectErr:           true,
			expectedState:       base.InstanceNotModified,
		},
		"non-version modify calls UpdateDomainConfig": {
			instance: &ElasticsearchInstance{
				Domain: "test-domain",
			},
			expectUpgradeCalled: false,
			expectedState:       base.InstanceInProgress,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			mock := &mockOpensearchClient{upgradeDomainErr: test.upgradeDomainErr}
			adapter := &dedicatedElasticsearchAdapter{
				ctx:        context.Background(),
				opensearch: mock,
				logger:     slog.New(&testutil.MockLogHandler{}),
			}

			state, err := adapter.modifyElasticsearch(test.instance)
			if test.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if state != test.expectedState {
				t.Errorf("expected state %v, got %v", test.expectedState, state)
			}
			upgradeCalled := mock.upgradeDomainInput != nil
			if upgradeCalled != test.expectUpgradeCalled {
				t.Errorf("UpgradeDomain called=%v, want=%v", upgradeCalled, test.expectUpgradeCalled)
			}
			if test.expectUpgradeCalled && mock.upgradeDomainInput != nil {
				if diff := deep.Equal(mock.upgradeDomainInput, &opensearch.UpgradeDomainInput{
					DomainName:    aws.String(test.instance.Domain),
					TargetVersion: aws.String(test.instance.TargetElasticsearchVersion),
				}); diff != nil {
					t.Error(diff)
				}
			}
		})
	}
}

func TestCheckCompatibleVersions(t *testing.T) {
	testCases := map[string]struct {
		targetVersion      string
		compatibleVersions []opensearchTypes.CompatibleVersionsMap
		apiErr             error
		expectErr          bool
		errContains        string
	}{
		"target in compatible list succeeds": {
			targetVersion: "OpenSearch_2.3",
			compatibleVersions: []opensearchTypes.CompatibleVersionsMap{
				{SourceVersion: aws.String("OpenSearch_1.3"), TargetVersions: []string{"OpenSearch_2.3"}},
			},
		},
		"target not in compatible list returns error with valid options": {
			targetVersion: "OpenSearch_3.0",
			compatibleVersions: []opensearchTypes.CompatibleVersionsMap{
				{SourceVersion: aws.String("OpenSearch_1.3"), TargetVersions: []string{"OpenSearch_2.3"}},
			},
			expectErr:   true,
			errContains: "OpenSearch_2.3",
		},
		"no compatible paths returns error": {
			targetVersion:      "OpenSearch_2.3",
			compatibleVersions: []opensearchTypes.CompatibleVersionsMap{},
			expectErr:          true,
			errContains:        "no upgrade paths are available",
		},
		"AWS API error is returned": {
			targetVersion: "OpenSearch_2.3",
			apiErr:        errors.New("AWS error"),
			expectErr:     true,
			errContains:   "AWS error",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			mock := &mockOpensearchClient{
				compatibleVersions:    test.compatibleVersions,
				compatibleVersionsErr: test.apiErr,
			}
			adapter := &dedicatedElasticsearchAdapter{
				ctx:        context.Background(),
				opensearch: mock,
				logger:     slog.New(&testutil.MockLogHandler{}),
			}

			err := adapter.checkCompatibleVersions("test-domain", test.targetVersion)
			if test.expectErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.errContains != "" && err != nil && !strings.Contains(err.Error(), test.errContains) {
				t.Errorf("expected error containing %q, got %q", test.errContains, err.Error())
			}
		})
	}
}
