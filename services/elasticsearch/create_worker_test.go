package elasticsearch

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/testutil"
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
		})
	}
}
