package elasticsearch

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

func TestCreateWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx              context.Context
		instance         *ElasticsearchInstance
		expectedInstance *ElasticsearchInstance
		expectedState    base.InstanceState
		password         string
		expectErr        bool
		worker           *CreateWorker
	}{
		"success": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			expectedInstance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
					State: base.InstanceReady,
				},
				AccessKey:              "fake-id",
				SecretKey:              "fake-secret",
				IamUserARN:             "user-arn",
				ARN:                    "arn",
				IamPolicy:              `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`,
				IamPolicyARN:           "user-policy-arn",
				BrokerSnapshotsEnabled: true,
				SnapshotARN:            "role-arn",
				IamPassRolePolicyARN:   "pass-role-policy-arn",
				SnapshotPolicyARN:      "snapshot-policy-arn",
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{
					createDomainOutput: &opensearch.CreateDomainOutput{
						DomainStatus: &opensearchTypes.DomainStatus{
							ARN: aws.String("arn"),
						},
					},
					describeDomainResults: []*opensearch.DescribeDomainOutput{
						{
							DomainStatus: &opensearchTypes.DomainStatus{
								Created: aws.Bool(true),
								Endpoints: map[string]string{
									"vpc": "endpoint",
								},
							},
						},
					},
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyOutput: []*iam.CreatePolicyOutput{
						{
							Policy: &types.Policy{
								Arn: aws.String("user-policy-arn"),
							},
						},
						{
							Policy: &types.Policy{
								Arn: aws.String("pass-role-policy-arn"),
							},
						},
						{
							Policy: &types.Policy{
								Arn: aws.String("snapshot-policy-arn"),
							},
						},
					},
					createRoleOutput: []*iam.CreateRoleOutput{
						{
							Role: &types.Role{
								Arn:      aws.String("role-arn"),
								RoleName: aws.String("role-name"),
							},
						},
					},
					getUserOutput: &iam.GetUserOutput{
						User: &types.User{
							Arn: aws.String("user-arn"),
						},
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{
					getCallerIdentityOutput: &sts.GetCallerIdentityOutput{
						Account: aws.String("account"),
					},
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedState: base.InstanceReady,
		},
		"error creating user": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{},
				&mockIamClient{
					createUserErr: errors.New("error creating user"),
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error creating user access keys": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{},
				&mockIamClient{
					createAccessKeyErr: errors.New("error creating access keys"),
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error getting user": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					getUserErr: errors.New("error getting user"),
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error getting caller identity": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					getUserOutput: &iam.GetUserOutput{
						User: &types.User{
							Arn: aws.String("user-arn"),
						},
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{
					getCallerIdentityErr: errors.New("error getting caller identity"),
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error creating domain": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{
					createDomainErr: errors.New("error creating domain"),
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					getUserOutput: &iam.GetUserOutput{
						User: &types.User{
							Arn: aws.String("user-arn"),
						},
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{
					getCallerIdentityOutput: &sts.GetCallerIdentityOutput{
						Account: aws.String("account"),
					},
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error creating IAM policy": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{
					createDomainOutput: &opensearch.CreateDomainOutput{
						DomainStatus: &opensearchTypes.DomainStatus{
							ARN: aws.String("arn"),
						},
					},
					describeDomainResults: []*opensearch.DescribeDomainOutput{
						{
							DomainStatus: &opensearchTypes.DomainStatus{
								Created: aws.Bool(true),
								Endpoints: map[string]string{
									"vpc": "endpoint",
								},
							},
						},
					},
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyErrs: []error{errors.New("error creating policy")},
					getUserOutput: &iam.GetUserOutput{
						User: &types.User{
							Arn: aws.String("user-arn"),
						},
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{
					getCallerIdentityOutput: &sts.GetCallerIdentityOutput{
						Account: aws.String("account"),
					},
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error attaching IAM policy": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{
					createDomainOutput: &opensearch.CreateDomainOutput{
						DomainStatus: &opensearchTypes.DomainStatus{
							ARN: aws.String("arn"),
						},
					},
					describeDomainResults: []*opensearch.DescribeDomainOutput{
						{
							DomainStatus: &opensearchTypes.DomainStatus{
								Created: aws.Bool(true),
								Endpoints: map[string]string{
									"vpc": "endpoint",
								},
							},
						},
					},
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyOutput: []*iam.CreatePolicyOutput{
						{
							Policy: &types.Policy{
								Arn: aws.String("user-policy-arn"),
							},
						},
						{
							Policy: &types.Policy{
								Arn: aws.String("pass-role-policy-arn"),
							},
						},
					},
					getUserOutput: &iam.GetUserOutput{
						User: &types.User{
							Arn: aws.String("user-arn"),
						},
					},
					attachUserPolicyErr: errors.New("error attaching user policy"),
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{
					getCallerIdentityOutput: &sts.GetCallerIdentityOutput{
						Account: aws.String("account"),
					},
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
		"error creating snapshot role": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				VolumeType:   "gp3",
				InstanceType: "t3.small.search",
				Instance: base.Instance{
					Uuid: uuid.NewString(),
					Request: request.Request{
						ServiceID: "aws-elasticsearch",
					},
				},
			},
			worker: NewCreateWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig:           &db.DBConfig{},
				},
				&mockOpensearchClient{
					createDomainOutput: &opensearch.CreateDomainOutput{
						DomainStatus: &opensearchTypes.DomainStatus{
							ARN: aws.String("arn"),
						},
					},
					describeDomainResults: []*opensearch.DescribeDomainOutput{
						{
							DomainStatus: &opensearchTypes.DomainStatus{
								Created: aws.Bool(true),
								Endpoints: map[string]string{
									"vpc": "endpoint",
								},
							},
						},
					},
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyOutput: []*iam.CreatePolicyOutput{
						{
							Policy: &types.Policy{
								Arn: aws.String("user-policy-arn"),
							},
						},
						{
							Policy: &types.Policy{
								Arn: aws.String("pass-role-policy-arn"),
							},
						},
					},
					getUserOutput: &iam.GetUserOutput{
						User: &types.User{
							Arn: aws.String("user-arn"),
						},
					},
					createRoleErrs: []error{
						errors.New("error creating role"),
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				&mockSTSClient{
					getCallerIdentityOutput: &sts.GetCallerIdentityOutput{
						Account: aws.String("account"),
					},
				},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectErr:     true,
			expectedState: base.InstanceNotCreated,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.worker.Work(test.ctx, &river.Job[CreateArgs]{Args: CreateArgs{
				Instance: test.instance,
			}})
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error")
			}
			if test.expectedInstance != nil {
				instance := &ElasticsearchInstance{}
				brokerDB.Where("uuid = ?", test.instance.Uuid).First(&instance)
				test.expectedInstance.Uuid = test.instance.Uuid
				if diff := deep.Equal(instance, test.expectedInstance); diff != nil {
					t.Error(diff)
				}
			}
			message, err := asyncmessage.GetLastAsyncJobMessage(brokerDB, test.instance.ServiceID, test.instance.Uuid, base.CreateOp)
			if err != nil {
				t.Fatal(err)
			}
			if message.JobState.State != test.expectedState {
				t.Fatalf("expected %s, got %s", test.expectedState, message.JobState.State)
			}
		})
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
		"audit + error logs enable FGAC and log publishing": {
			esInstance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				DataCount:                  2,
				SubnetID3AZ1:               "az-3",
				SubnetID4AZ2:               "az-4",
				SecGroup:                   "group-1",
				EncryptAtRest:              true,
				VolumeSize:                 10,
				VolumeType:                 "gp3",
				InstanceType:               "m5.2xlarge.search",
				NodeToNodeEncryption:       true,
				AutomatedSnapshotStartHour: 0,
				ErrorLogsEnabled:           true,
				AuditLogsEnabled:           true,
				AdvancedSecurityEnabled:    true,
				IamUserARN:                 "arn:aws-us-gov:iam::123456789012:user/test-domain",
				ErrorLogsGroupARN:          "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/test-domain/application-logs",
				AuditLogsGroupARN:          "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/test-domain/audit-logs",
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
					Enabled: aws.Bool(true),
				},
				AdvancedSecurityOptions: &opensearchTypes.AdvancedSecurityOptionsInput{
					Enabled:                     aws.Bool(true),
					InternalUserDatabaseEnabled: aws.Bool(false),
					MasterUserOptions: &opensearchTypes.MasterUserOptions{
						MasterUserARN: aws.String("arn:aws-us-gov:iam::123456789012:user/test-domain"),
					},
				},
				LogPublishingOptions: map[string]opensearchTypes.LogPublishingOption{
					"AUDIT_LOGS": {
						CloudWatchLogsLogGroupArn: aws.String("arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/test-domain/audit-logs"),
						Enabled:                   aws.Bool(true),
					},
					"ES_APPLICATION_LOGS": {
						CloudWatchLogsLogGroupArn: aws.String("arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/test-domain/application-logs"),
						Enabled:                   aws.Bool(true),
					},
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
