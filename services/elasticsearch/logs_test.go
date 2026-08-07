package elasticsearch

import (
	"context"
	"log/slog"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
)

func TestApplyLogOptions(t *testing.T) {
	testCases := map[string]struct {
		initial  ElasticsearchInstance
		options  ElasticsearchLogOptions
		expected ElasticsearchInstance
	}{
		"create with nothing set leaves everything off": {
			options:  ElasticsearchLogOptions{},
			expected: ElasticsearchInstance{},
		},
		"enable error logs only": {
			options:  ElasticsearchLogOptions{ErrorLogs: aws.Bool(true)},
			expected: ElasticsearchInstance{ErrorLogsEnabled: true},
		},
		"enabling audit logs turns on FGAC": {
			options: ElasticsearchLogOptions{AuditLogs: aws.Bool(true)},
			expected: ElasticsearchInstance{
				AuditLogsEnabled:        true,
				AdvancedSecurityEnabled: true,
			},
		},
		"nil pointers on modify leave existing settings unchanged": {
			initial: ElasticsearchInstance{
				ErrorLogsEnabled:        true,
				AuditLogsEnabled:        true,
				AdvancedSecurityEnabled: true,
			},
			options: ElasticsearchLogOptions{},
			expected: ElasticsearchInstance{
				ErrorLogsEnabled:        true,
				AuditLogsEnabled:        true,
				AdvancedSecurityEnabled: true,
			},
		},
		"disabling audit logs leaves FGAC enabled (one way op)": {
			initial: ElasticsearchInstance{
				AuditLogsEnabled:        true,
				AdvancedSecurityEnabled: true,
			},
			options: ElasticsearchLogOptions{AuditLogs: aws.Bool(false)},
			expected: ElasticsearchInstance{
				AuditLogsEnabled:        false,
				AdvancedSecurityEnabled: true,
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			i := test.initial
			i.applyLogOptions(test.options)
			if diff := deep.Equal(i, test.expected); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestValidateAuditLogSupport(t *testing.T) {
	testCases := map[string]struct {
		instance  *ElasticsearchInstance
		expectErr bool
	}{
		"no audit logs is allowed": {
			instance:  &ElasticsearchInstance{},
			expectErr: false,
		},
		"no audit logs is allowed even without encryption": {
			instance: &ElasticsearchInstance{
				NodeToNodeEncryption: false,
				EncryptAtRest:        false,
			},
			expectErr: false,
		},
		"audit logs allowed when plan enables both encryptions": {
			instance: &ElasticsearchInstance{
				AdvancedSecurityEnabled: true,
				NodeToNodeEncryption:    true,
				EncryptAtRest:           true,
			},
			expectErr: false,
		},
		"audit logs rejected without node-to-node encryption": {
			instance: &ElasticsearchInstance{
				AdvancedSecurityEnabled: true,
				NodeToNodeEncryption:    false,
				EncryptAtRest:           true,
			},
			expectErr: true,
		},
		"audit logs rejected without encryption at rest": {
			instance: &ElasticsearchInstance{
				AdvancedSecurityEnabled: true,
				NodeToNodeEncryption:    true,
				EncryptAtRest:           false,
			},
			expectErr: true,
		},
		"audit logs rejected": {
			instance: &ElasticsearchInstance{
				AdvancedSecurityEnabled: true,
				NodeToNodeEncryption:    false,
				EncryptAtRest:           false,
			},
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.instance.validateAuditLogSupport()
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !test.expectErr && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}

func TestLogGroupName(t *testing.T) {
	got := logGroupName("cg-broker-abc", "audit-logs")
	expected := "/aws/OpenSearchService/domains/cg-broker-abc/audit-logs"
	if got != expected {
		t.Errorf("logGroupName: got %q, expected %q", got, expected)
	}
}

func TestBuildLogPublishingOptions(t *testing.T) {
	testCases := map[string]struct {
		instance *ElasticsearchInstance
		expected map[string]opensearchTypes.LogPublishingOption
	}{
		"no groups created yields empty map": {
			instance: &ElasticsearchInstance{},
			expected: map[string]opensearchTypes.LogPublishingOption{},
		},
		"enabled type with group is published enabled": {
			instance: &ElasticsearchInstance{
				ErrorLogsEnabled:  true,
				ErrorLogsGroupARN: "arn:test",
			},
			expected: map[string]opensearchTypes.LogPublishingOption{
				"ES_APPLICATION_LOGS": {
					CloudWatchLogsLogGroupArn: aws.String("arn:test"),
					Enabled:                   aws.Bool(true),
				},
			},
		},
		"disabled type that still has a group is published disabled": {
			instance: &ElasticsearchInstance{
				ErrorLogsEnabled:  false,
				ErrorLogsGroupARN: "arn:test",
			},
			expected: map[string]opensearchTypes.LogPublishingOption{
				"ES_APPLICATION_LOGS": {
					CloudWatchLogsLogGroupArn: aws.String("arn:test"),
					Enabled:                   aws.Bool(false),
				},
			},
		},
		"enabled type without a group ARN is skipped": {
			instance: &ElasticsearchInstance{
				SearchSlowLogsEnabled: true,
			},
			expected: map[string]opensearchTypes.LogPublishingOption{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			options := buildLogPublishingOptions(test.instance)
			if diff := deep.Equal(options, test.expected); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestAdvancedSecurityOptionsForAudit(t *testing.T) {
	testCases := map[string]struct {
		instance  *ElasticsearchInstance
		expected  *opensearchTypes.AdvancedSecurityOptionsInput
		expectErr bool
	}{
		"FGAC disabled returns nil": {
			instance: &ElasticsearchInstance{},
			expected: nil,
		},
		"FGAC enabled without IamUserARN is an error": {
			instance:  &ElasticsearchInstance{AdvancedSecurityEnabled: true},
			expectErr: true,
		},
		"FGAC enabled with IamUserARN sets the master user": {
			instance: &ElasticsearchInstance{
				AdvancedSecurityEnabled: true,
				IamUserARN:              "arn:test",
			},
			expected: &opensearchTypes.AdvancedSecurityOptionsInput{
				Enabled:                     aws.Bool(true),
				InternalUserDatabaseEnabled: aws.Bool(false),
				MasterUserOptions: &opensearchTypes.MasterUserOptions{
					MasterUserARN: aws.String("arn:test"),
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := advancedSecurityOptionsForAudit(test.instance)
			if test.expectErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if diff := deep.Equal(got, test.expected); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestEnsureLogGroups(t *testing.T) {
	logger := slog.New(&testutil.MockLogHandler{})

	const (
		applicationLogGroup = "/aws/OpenSearchService/domains/cg-broker-abc/application-logs"
		auditLogGroup       = "/aws/OpenSearchService/domains/cg-broker-abc/audit-logs"
		applicationLogArn   = "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:" + applicationLogGroup
		auditLogArn         = "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:" + auditLogGroup
	)

	testCases := map[string]struct {
		initial             *ElasticsearchInstance
		createLogGroupError error
		expectedCreated     []string
		expectedRetention   []string
		expected            *ElasticsearchInstance
	}{
		"create groups, sets retention and ARNs for enabled types": {
			initial: &ElasticsearchInstance{
				Domain:           "cg-broker-abc",
				ErrorLogsEnabled: true,
				AuditLogsEnabled: true,
			},
			expectedCreated:   []string{auditLogGroup, applicationLogGroup},
			expectedRetention: []string{auditLogGroup, applicationLogGroup},
			expected: &ElasticsearchInstance{
				Domain:            "cg-broker-abc",
				ErrorLogsEnabled:  true,
				AuditLogsEnabled:  true,
				ErrorLogsGroupARN: applicationLogArn,
				AuditLogsGroupARN: auditLogArn,
			},
		},
		"skips types whose ARN is already set": {
			initial: &ElasticsearchInstance{
				Domain:            "cg-broker-abc",
				ErrorLogsEnabled:  true,
				ErrorLogsGroupARN: "arn:already-set",
			},
			expectedCreated:   nil,
			expectedRetention: nil,
			expected: &ElasticsearchInstance{
				Domain:            "cg-broker-abc",
				ErrorLogsEnabled:  true,
				ErrorLogsGroupARN: "arn:already-set",
			},
		},
		"treats already existing log group as a success": {
			initial: &ElasticsearchInstance{
				Domain:           "cg-broker-abc",
				ErrorLogsEnabled: true,
			},
			createLogGroupError: &cloudwatchTypes.ResourceAlreadyExistsException{},
			expectedCreated:     nil,
			expectedRetention:   []string{applicationLogGroup},
			expected: &ElasticsearchInstance{
				Domain:            "cg-broker-abc",
				ErrorLogsEnabled:  true,
				ErrorLogsGroupARN: applicationLogArn,
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			client := &mockCloudwatchLogsClient{createLogGroupErr: test.createLogGroupError}

			err := ensureLogGroups(context.Background(), client, logger, test.initial, 3, "us-gov-west-1", "123456789012")
			if err != nil {
				t.Fatal(err)
			}

			gotCreated := append([]string{}, client.createdLogGroups...)
			wantCreated := append([]string{}, test.expectedCreated...)
			sort.Strings(gotCreated)
			sort.Strings(wantCreated)
			if diff := deep.Equal(gotCreated, wantCreated); diff != nil {
				t.Errorf("createdLogGroups: %v", diff)
			}

			gotRetention := append([]string{}, client.putRetentionLogGroups...)
			wantRetention := append([]string{}, test.expectedRetention...)
			sort.Strings(gotRetention)
			sort.Strings(wantRetention)
			if diff := deep.Equal(gotRetention, wantRetention); diff != nil {
				t.Errorf("putRetentionLogGroups: %v", diff)
			}

			if diff := deep.Equal(test.initial, test.expected); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestCleanupLogGroups(t *testing.T) {
	logger := slog.New(&testutil.MockLogHandler{})

	testCases := map[string]struct {
		instance          *ElasticsearchInstance
		expectedDeleted   []string
		deleteLogGroupErr error
		expectErr         bool
	}{
		"deletes only log groups that were created": {
			instance: &ElasticsearchInstance{
				ErrorLogsGroupARN: "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/cg-broker-abc/application-logs",
				AuditLogsGroupARN: "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/cg-broker-abc/audit-logs",
			},
			expectedDeleted: []string{
				"/aws/OpenSearchService/domains/cg-broker-abc/application-logs",
				"/aws/OpenSearchService/domains/cg-broker-abc/audit-logs",
			},
		},
		"no groups created deletes nothing": {
			instance:        &ElasticsearchInstance{},
			expectedDeleted: nil,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			test.instance.Domain = "cg-broker-abc"
			client := &mockCloudwatchLogsClient{deleteLogGroupErr: test.deleteLogGroupErr}

			err := cleanupLogGroups(context.Background(), client, logger, test.instance)
			if err != nil && !test.expectErr {
				t.Fatal(err)
			}

			got := append([]string{}, client.deletedLogGroups...)
			exp := append([]string{}, test.expectedDeleted...)
			sort.Strings(got)
			sort.Strings(exp)
			if diff := deep.Equal(got, exp); diff != nil {
				t.Error(diff)
			}
		})
	}
}
