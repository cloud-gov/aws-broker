package elasticsearch

import (
	"context"
	"log/slog"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/go-test/deep"
)

func TestApplyLogOptions(t *testing.T) {
	testCases := map[string]struct {
		initial                  ElasticsearchInstance
		options                  ElasticsearchLogOptions
		expectedAudit            bool
		expectedError            bool
		expectedSearchSlow       bool
		expectedIndexSlow        bool
		expectedAdvancedSecurity bool
	}{
		"create with nothing set leaves everything off": {
			options: ElasticsearchLogOptions{},
		},
		"enable error logs only": {
			options:       ElasticsearchLogOptions{ErrorLogs: aws.Bool(true)},
			expectedError: true,
		},
		"enabling audit logs turns on FGAC": {
			options:                  ElasticsearchLogOptions{AuditLogs: aws.Bool(true)},
			expectedAudit:            true,
			expectedAdvancedSecurity: true,
		},
		"nil pointers on modify leave existing settings unchanged": {
			initial: ElasticsearchInstance{
				ErrorLogsEnabled:        true,
				AuditLogsEnabled:        true,
				AdvancedSecurityEnabled: true,
			},
			options:                  ElasticsearchLogOptions{},
			expectedError:            true,
			expectedAudit:            true,
			expectedAdvancedSecurity: true,
		},
		"disabling audit logs leaves FGAC enabled (one way op)": {
			initial: ElasticsearchInstance{
				AuditLogsEnabled:        true,
				AdvancedSecurityEnabled: true,
			},
			options:                  ElasticsearchLogOptions{AuditLogs: aws.Bool(false)},
			expectedAudit:            false,
			expectedAdvancedSecurity: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			i := test.initial
			i.applyLogOptions(test.options)
			if i.AuditLogsEnabled != test.expectedAudit {
				t.Errorf("AuditLogsEnabled: got %v, expected %v", i.AuditLogsEnabled, test.expectedAudit)
			}
			if i.ErrorLogsEnabled != test.expectedError {
				t.Errorf("ErrorLogsEnabled: got %v, expected %v", i.ErrorLogsEnabled, test.expectedError)
			}
			if i.SearchSlowLogsEnabled != test.expectedSearchSlow {
				t.Errorf("SearchSlowLogsEnabled: got %v, expected %v", i.SearchSlowLogsEnabled, test.expectedSearchSlow)
			}
			if i.IndexSlowLogsEnabled != test.expectedIndexSlow {
				t.Errorf("IndexSlowLogsEnabled: got %v, expected %v", i.IndexSlowLogsEnabled, test.expectedIndexSlow)
			}
			if i.AdvancedSecurityEnabled != test.expectedAdvancedSecurity {
				t.Errorf("AdvancedSecurityEnabled: got %v, expected %v", i.AdvancedSecurityEnabled, test.expectedAdvancedSecurity)
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
		instance     *ElasticsearchInstance
		expectedKeys map[string]bool // log type -> expected enable value
	}{
		"no groups created yields empty map": {
			instance:     &ElasticsearchInstance{},
			expectedKeys: map[string]bool{},
		},
		"enabled type with group is published enabled": {
			instance: &ElasticsearchInstance{
				ErrorLogsEnabled:  true,
				ErrorLogsGroupARN: "arn:test",
			},
			expectedKeys: map[string]bool{"ES_APPLICATION_LOGS": true},
		},
		"disabled type that still has a group is published disabled": {
			instance: &ElasticsearchInstance{
				ErrorLogsEnabled:  false,
				ErrorLogsGroupARN: "arn:test",
			},
			expectedKeys: map[string]bool{"ES_APPLICATION_LOGS": false},
		},
		"enabled type without a group ARN is skipped": {
			instance: &ElasticsearchInstance{
				SearchSlowLogsEnabled: true,
			},
			expectedKeys: map[string]bool{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			options := buildLogPublishingOptions(test.instance)
			if len(options) != len(test.expectedKeys) {
				t.Fatalf("got %d options, expected %d: %+v", len(options), len(test.expectedKeys), options)
			}
			for key, wantEnabled := range test.expectedKeys {
				opt, ok := options[key]
				if !ok {
					t.Fatalf("expected log type %q in options", key)
				}
				if opt.Enabled == nil || *opt.Enabled != wantEnabled {
					t.Errorf("%s Enabled: got %v, want %v", key, opt.Enabled, wantEnabled)
				}
				if opt.CloudWatchLogsLogGroupArn == nil || *opt.CloudWatchLogsLogGroupArn == "" {
					t.Errorf("%s expected a non-empty log group ARN", key)
				}
			}
		})
	}
}

func TestAdvancedSecurityOptionsForAudit(t *testing.T) {
	got, err := advancedSecurityOptionsForAudit(&ElasticsearchInstance{})
	if err != nil {
		t.Fatalf("unexpected error when FGAC disabled: %s", err)
	}
	if got != nil {
		t.Errorf("expected nil when FGAC disabled, got %+v", got)
	}

	if _, err := advancedSecurityOptionsForAudit(&ElasticsearchInstance{
		AdvancedSecurityEnabled: true,
	}); err == nil {
		t.Error("expected an error when FGAC is enabled but IamUserARN is empty, got nil")
	}

	options, err := advancedSecurityOptionsForAudit(&ElasticsearchInstance{
		AdvancedSecurityEnabled: true,
		IamUserARN:              "arn:aws-us-gov:iam::123456789012:user/test-domain",
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if options == nil {
		t.Fatal("expected non-nil advanced security options")
	}
	if options.MasterUserOptions == nil || *options.MasterUserOptions.MasterUserARN != "arn:aws-us-gov:iam::123456789012:user/test-domain" {
		t.Errorf("expected IAM user as master user ARN, got %+v", options.MasterUserOptions)
	}
	if options.InternalUserDatabaseEnabled == nil || *options.InternalUserDatabaseEnabled {
		t.Error("expected internal user database to be disabled")
	}
}

func TestEnsureLogGroups(t *testing.T) {
	logger := slog.New(&testutil.MockLogHandler{})

	t.Run("creates groups, sets retention and ARNS for enabled types", func(t *testing.T) {
		client := &mockCloudwatchLogsClient{}
		i := &ElasticsearchInstance{
			Domain:           "cg-broker-abc",
			ErrorLogsEnabled: true,
			AuditLogsEnabled: true,
		}

		err := ensureLogGroups(context.Background(), client, logger, i, 3, "us-gov-west-1", "123456789012")
		if err != nil {
			t.Fatal(err)
		}

		if len(client.createdLogGroups) != 2 {
			t.Errorf("expected 2 created log groups, got %v", client.createdLogGroups)
		}
		if len(client.putRetentionLogGroups) != 2 {
			t.Errorf("expected retention set on 2 log groups, got %v", client.putRetentionLogGroups)
		}
		expectedErrorArn := "arn:aws-us-gov:logs:us-gov-west-1:123456789012:log-group:/aws/OpenSearchService/domains/cg-broker-abc/application-logs"
		if i.ErrorLogsGroupARN != expectedErrorArn {
			t.Errorf("ErrorLogGroupsARN: got %q, expected %q", i.ErrorLogsGroupARN, expectedErrorArn)
		}
		if i.AuditLogsGroupARN == "" {
			t.Error("expected AuditLogsGroupARN to be set")
		}
		if i.SearchSlowLogsGroupARN != "" || i.IndexSlowLogsGroupARN != "" {
			t.Error("did not expect groups for disabled log types")
		}
	})

	t.Run("skips types whose group ARN is already set", func(t *testing.T) {
		client := &mockCloudwatchLogsClient{}
		i := &ElasticsearchInstance{
			Domain:            "cg-broker-abc",
			ErrorLogsEnabled:  true,
			ErrorLogsGroupARN: "arn:already-set",
		}

		err := ensureLogGroups(context.Background(), client, logger, i, 3, "us-gov-west-1", "123456789012")
		if err != nil {
			t.Fatal(err)
		}

		if len(client.createdLogGroups) != 0 {
			t.Errorf("expected 0 created log groups, got %v", client.createdLogGroups)
		}
		if i.ErrorLogsGroupARN != "arn:already-set" {
			t.Errorf("expected existing ARN preserved, got %q", i.ErrorLogsGroupARN)
		}
	})

	t.Run("treates an already-existing log group as success", func(t *testing.T) {
		client := &mockCloudwatchLogsClient{
			createLogGroupErr: &cloudwatchTypes.ResourceAlreadyExistsException{},
		}
		i := &ElasticsearchInstance{
			Domain:           "cg-broker-abc",
			ErrorLogsEnabled: true,
		}

		err := ensureLogGroups(context.Background(), client, logger, i, 3, "us-gov-west-1", "123456789012")
		if err != nil {
			t.Fatalf("expected AlreadyExists exception to pass without error, got %s", err)
		}
		if i.ErrorLogsGroupARN == "" {
			t.Error("expectedARN to be set even when the group already existed")
		}
	})
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
