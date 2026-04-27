package elasticsearch

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/testutil"
)

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
