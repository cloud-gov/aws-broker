package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/riverqueue/river"
)

type mockEsApiClient struct {
	getSnapshotStatusCallNum   int
	getSnapshotStatusResponses []string
	getSnapshotStatusErrs      []error
}

func (m *mockEsApiClient) CreateSnapshotRepo(repositoryName string, bucketName string, path string, region string, roleArn string) (string, error) {
	return "", nil
}

func (m *mockEsApiClient) CreateSnapshot(repositoryName string, snapshotName string) (string, error) {
	return "", nil
}

func (m *mockEsApiClient) GetSnapshotStatus(repositoryName string, snapshotName string) (string, error) {
	currentCallNum := m.getSnapshotStatusCallNum
	m.getSnapshotStatusCallNum++
	if len(m.getSnapshotStatusErrs) > 0 && m.getSnapshotStatusErrs[currentCallNum] != nil {
		return "", m.getSnapshotStatusErrs[currentCallNum]
	}
	status := m.getSnapshotStatusResponses[currentCallNum]
	return status, nil
}

type mockOpensearchClient struct {
	describeDomainCallNum int
	describeDomainErrs    []error
	describeDomainResults []*opensearch.DescribeDomainOutput
}

func (o *mockOpensearchClient) CreateDomain(ctx context.Context, params *opensearch.CreateDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.CreateDomainOutput, error) {
	return nil, nil
}

func (o *mockOpensearchClient) DeleteDomain(ctx context.Context, params *opensearch.DeleteDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DeleteDomainOutput, error) {
	return nil, nil
}

func (o *mockOpensearchClient) DescribeDomain(ctx context.Context, params *opensearch.DescribeDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DescribeDomainOutput, error) {
	if len(o.describeDomainErrs) > 0 && o.describeDomainErrs[o.describeDomainCallNum] != nil {
		return nil, o.describeDomainErrs[o.describeDomainCallNum]
	}
	result := o.describeDomainResults[o.describeDomainCallNum]
	o.describeDomainCallNum++
	return result, nil
}

func (o *mockOpensearchClient) UpdateDomainConfig(ctx context.Context, params *opensearch.UpdateDomainConfigInput, optFns ...func(*opensearch.Options)) (*opensearch.UpdateDomainConfigOutput, error) {
	return nil, nil
}

type mockS3Client struct {
	putObjectErr error
}

func (s *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, s.putObjectErr
}

type mockIamClient struct {
	createPolicyOutput       *iam.CreatePolicyOutput
	createRoleCallNum        int
	createRoleOutput         []*iam.CreateRoleOutput
	listPolicyVersionsOutput *iam.ListPolicyVersionsOutput
}

func (m *mockIamClient) CreateAccessKey(ctx context.Context, params *iam.CreateAccessKeyInput, optFns ...func(*iam.Options)) (*iam.CreateAccessKeyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) CreatePolicy(ctx context.Context, params *iam.CreatePolicyInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
	return m.createPolicyOutput, nil
}

func (m *mockIamClient) DeleteAccessKey(ctx context.Context, params *iam.DeleteAccessKeyInput, optFns ...func(*iam.Options)) (*iam.DeleteAccessKeyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeleteRole(ctx context.Context, params *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeleteUser(ctx context.Context, params *iam.DeleteUserInput, optFns ...func(*iam.Options)) (*iam.DeleteUserOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DetachRolePolicy(ctx context.Context, params *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DetachUserPolicy(ctx context.Context, params *iam.DetachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) AttachRolePolicy(ctx context.Context, params *iam.AttachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) AttachUserPolicy(ctx context.Context, params *iam.AttachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.AttachUserPolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) CreatePolicyVersion(ctx context.Context, params *iam.CreatePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	return nil, nil
}

func (m *mockIamClient) CreateRole(ctx context.Context, params *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	output := m.createRoleOutput[m.createRoleCallNum]
	m.createRoleCallNum++
	return output, nil
}

func (m *mockIamClient) CreateUser(ctx context.Context, params *iam.CreateUserInput, optFns ...func(*iam.Options)) (*iam.CreateUserOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeletePolicy(ctx context.Context, params *iam.DeletePolicyInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeletePolicyVersion(ctx context.Context, params *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetPolicy(ctx context.Context, params *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetPolicyVersion(ctx context.Context, params *iam.GetPolicyVersionInput, optFns ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetRole(ctx context.Context, params *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetUser(ctx context.Context, params *iam.GetUserInput, optFns ...func(*iam.Options)) (*iam.GetUserOutput, error) {
	return nil, nil
}

func (m *mockIamClient) ListAttachedRolePolicies(ctx context.Context, params *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	return nil, nil
}

func (m *mockIamClient) ListAttachedUserPolicies(ctx context.Context, params *iam.ListAttachedUserPoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedUserPoliciesOutput, error) {
	return nil, nil
}

func (m *mockIamClient) ListPolicyVersions(ctx context.Context, params *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	return m.listPolicyVersionsOutput, nil
}

func TestDeleteWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	snapshotRepo := "test-repo"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if r.RequestURI == fmt.Sprintf("/_snapshot/%s", snapshotRepo) {
			fmt.Fprintln(w, `{"status": "ok"}`)
		} else if strings.HasPrefix(r.RequestURI, fmt.Sprintf("/_snapshot/%s", snapshotRepo)) {
			fmt.Fprintln(w, `{"snapshots": [{"state":"SUCCESS"}]}`)
		}
	}))
	defer ts.Close()
	testApiUrl, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx           context.Context
		instance      *ElasticsearchInstance
		expectedState base.InstanceState
		password      string
		expectErr     bool
		worker        *DeleteWorker
	}{
		"success": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
					Host: testApiUrl.Host,
				},
				AccessKey: "fake-key",
				SecretKey: "fake-secret",
				Protocol:  testApiUrl.Scheme,
			},
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMaxDuration: 1 * time.Millisecond,
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxRetries:  1,
					DbConfig: &db.DBConfig{
						DbType: "sqlite3",
					},
					Region:            "fake-region",
					SnapshotsRepoName: snapshotRepo,
				},
				&mockOpensearchClient{
					describeDomainErrs: []error{&types.ResourceNotFoundException{}},
				},
				&mockIamClient{
					createRoleOutput: []*iam.CreateRoleOutput{
						{
							Role: &iamTypes.Role{
								Arn: aws.String("role1"),
							},
						},
						{
							Role: &iamTypes.Role{
								Arn: aws.String("role2"),
							},
						},
					},
					createPolicyOutput: &iam.CreatePolicyOutput{
						Policy: &iamTypes.Policy{
							Arn: aws.String("policy-1"),
						},
					},
					listPolicyVersionsOutput: &iam.ListPolicyVersionsOutput{
						Versions: []iamTypes.PolicyVersion{},
					},
				},
				&mockS3Client{},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedState: base.InstanceReady,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err = test.worker.Work(test.ctx, &river.Job[DeleteArgs]{Args: DeleteArgs{
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

func TestPollForSnapshotCreation(t *testing.T) {
	testCases := map[string]struct {
		esApiClient              EsApiClient
		worker                   *DeleteWorker
		expectedGetSnapshotCalls int
		expectErr                bool
	}{
		"success": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusResponses: []string{"SUCCESS"},
			},
			worker: NewDeleteWorker(
				nil,
				&config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 1,
				},
				nil,
				nil,
				nil,
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedGetSnapshotCalls: 1,
		},
		"success with retries": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusResponses: []string{"IN PROGRESS", "IN PROGRESS", "SUCCESS"},
			},
			worker: NewDeleteWorker(
				nil,
				&config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 3,
				},
				nil,
				nil,
				nil,
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedGetSnapshotCalls: 3,
		},
		"gives up after maximum retries": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusResponses: []string{"IN PROGRESS", "IN PROGRESS", "IN PROGRESS"},
			},
			worker: NewDeleteWorker(
				nil,
				&config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 3,
				},
				nil,
				nil,
				nil,
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedGetSnapshotCalls: 3,
			expectErr:                true,
		},
		"error getting snapshot status": {
			esApiClient: &mockEsApiClient{
				getSnapshotStatusErrs: []error{errors.New("error getting snapshot status")},
			},
			worker: NewDeleteWorker(
				nil,
				&config.Settings{
					PollAwsMinDelay:   1 * time.Millisecond,
					PollAwsMaxRetries: 1,
				},
				nil,
				nil,
				nil,
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedGetSnapshotCalls: 1,
			expectErr:                true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.worker.pollForSnapshotCreation(test.esApiClient, "foobar")
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}
			if test.expectErr && err == nil {
				t.Fatal("expected error")
			}
			if mockEsApiClient, ok := test.esApiClient.(*mockEsApiClient); ok {
				if mockEsApiClient.getSnapshotStatusCallNum != test.expectedGetSnapshotCalls {
					t.Fatalf("expected %d GetSnapshotStatus calls, got %d", test.expectedGetSnapshotCalls, mockEsApiClient.getSnapshotStatusCallNum)
				}
			}
		})
	}
}
