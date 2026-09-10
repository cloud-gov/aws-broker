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
	"github.com/google/uuid"
	"github.com/riverqueue/river"
)

func TestCreateWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		instance      *ElasticsearchInstance
		expectedState base.InstanceState
		password      string
		expectErr     bool
		worker        *CreateWorker
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
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyOutput: &iam.CreatePolicyOutput{
						Policy: &types.Policy{
							Arn: aws.String("policy-arn"),
						},
					},
					createRoleOutput: []*iam.CreateRoleOutput{
						{
							Role: &types.Role{
								Arn: aws.String("role-arn"),
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
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyErr: errors.New("error creating policy"),
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
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyOutput: &iam.CreatePolicyOutput{
						Policy: &types.Policy{
							Arn: aws.String("policy-arn"),
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
				},
				&mockIamClient{
					createAccessKeyOutput: &iam.CreateAccessKeyOutput{
						AccessKey: &types.AccessKey{
							AccessKeyId:     aws.String("fake-id"),
							SecretAccessKey: aws.String("fake-secret"),
						},
					},
					createPolicyOutput: &iam.CreatePolicyOutput{
						Policy: &types.Policy{
							Arn: aws.String("policy-arn"),
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
