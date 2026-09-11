package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/db"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/riverqueue/river"
)

func TestDeleteWorkerWork(t *testing.T) {
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	snapshotRepo := "test-repo"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if r.RequestURI == fmt.Sprintf("/_snapshot/%s", snapshotRepo) {
			fmt.Fprintln(w, `{"status": "ok"}`) //nolint:errcheck // test fixture writer; Fprintln to a test buffer
		} else if strings.HasPrefix(r.RequestURI, fmt.Sprintf("/_snapshot/%s", snapshotRepo)) {
			fmt.Fprintln(w, `{"snapshots": [{"state":"SUCCESS"}]}`) //nolint:errcheck // test fixture writer; Fprintln to a test buffer
		}
	}))
	defer ts.Close()
	testApiUrl, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	testApiPort, err := strconv.ParseInt(testApiUrl.Port(), 10, 64)
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
					Host: testApiUrl.Hostname(),
					Port: testApiPort,
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
					createPolicyOutput: []*iam.CreatePolicyOutput{
						{
							Policy: &iamTypes.Policy{
								Arn: aws.String("policy-1"),
							},
						},
					},
					listPolicyVersionsOutput: &iam.ListPolicyVersionsOutput{
						Versions: []iamTypes.PolicyVersion{},
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
				slog.New(&testutil.MockLogHandler{}),
			),
			expectedState: base.InstanceReady,
		},
		"instance has no host": {
			ctx:      t.Context(),
			password: helpers.RandStr(10),
			instance: &ElasticsearchInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
					Port: testApiPort,
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
					describeDomainResults: []*opensearch.DescribeDomainOutput{
						{
							DomainStatus: &types.DomainStatus{
								Created: aws.Bool(true),
								Endpoints: map[string]string{
									"vpc": testApiUrl.Hostname(),
								},
								ARN:           aws.String("fake-arn"),
								EngineVersion: aws.String("version"),
							},
						},
					},
					describeDomainErrs: []error{nil, &types.ResourceNotFoundException{}},
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
					createPolicyOutput: []*iam.CreatePolicyOutput{
						{
							Policy: &iamTypes.Policy{
								Arn: aws.String("policy-1"),
							},
						},
					},
					listPolicyVersionsOutput: &iam.ListPolicyVersionsOutput{
						Versions: []iamTypes.PolicyVersion{},
					},
				},
				&mockS3Client{},
				&mockCloudwatchLogsClient{},
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
