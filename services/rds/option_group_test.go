package rds

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/testutil"
)

func newTestOptionGroupClient(rdsClient RDSClientInterface) *awsOptionsGroupClient {
	return NewAwsOptionGroupClient(
		context.TODO(),
		rdsClient,
		&config.Settings{
			PollAwsMinDelay:   1 * time.Millisecond,
			PollAwsMaxRetries: 1,
		},
		slog.New(&testutil.MockLogHandler{}),
	)
}

func TestIsCustomOptionGroup(t *testing.T) {
	o := newTestOptionGroupClient(&mockRDSClient{})
	testCases := map[string]struct {
		optionGroupName string
		expected        bool
	}{
		"empty":         {"", false},
		"default group": {"default:mysql-8-0", false},
		"manual group":  {"my-audit-group", true},
		"broker group":  {"cg-aws-broker-db1-version-8-4-9", true},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if got := o.IsCustomOptionGroup(tc.optionGroupName); got != tc.expected {
				t.Errorf("IsCustomOptionGroup(%q) = %v, expected %v", tc.optionGroupName, got, tc.expected)
			}
		})
	}
}

func TestIsBrokerOptionGroup(t *testing.T) {
	o := newTestOptionGroupClient(&mockRDSClient{})
	if o.IsBrokerOptionGroup("my-audit-group") {
		t.Error("expected manual group to not be a broker group")
	}
	if !o.IsBrokerOptionGroup("cg-aws-broker-db1-version-8-4-9") {
		t.Error("expected broker-prefixed group to be a broker group")
	}
}

func TestReconcileRDSInstanceOptionGroup(t *testing.T) {
	o := newTestOptionGroupClient(&mockRDSClient{})
	testCases := map[string]struct {
		dbInstanceState         *rdsTypes.DBInstance
		instance                RDSInstance
		expectedOptionGroupName string
	}{
		"no memberships leaves option group unset": {
			dbInstanceState:         &rdsTypes.DBInstance{},
			instance:                RDSInstance{},
			expectedOptionGroupName: "",
		},
		"default option group is ignored": {
			dbInstanceState: &rdsTypes.DBInstance{
				OptionGroupMemberships: []rdsTypes.OptionGroupMembership{
					{OptionGroupName: aws.String("default:mysql-8-0")},
				},
			},
		},
		"custom option group is captured": {
			dbInstanceState: &rdsTypes.DBInstance{
				OptionGroupMemberships: []rdsTypes.OptionGroupMembership{
					{OptionGroupName: aws.String("my-audit-group")},
				},
			},
			instance:                RDSInstance{},
			expectedOptionGroupName: "my-audit-group",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reconciled, err := o.ReconcileRDSInstanceOptionGroup(tc.dbInstanceState, tc.instance)
			if err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
			if reconciled.OptionGroupName != tc.expectedOptionGroupName {
				t.Errorf("expected OptiongroupName %q, got %q", tc.expectedOptionGroupName, reconciled.OptionGroupName)
			}
		})
	}
}

func TestProvisionOrModifyCustomOptionGroup_MajorUpgrade(t *testing.T) {
	optionGroupNotFound := &rdsTypes.OptionGroupNotFoundFault{}
	mockRDS := &mockRDSClient{
		describeOptionGroupsErrs: []error{nil, optionGroupNotFound},
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{
						OptionGroupName:    aws.String("my-audit-group"),
						MajorEngineVersion: aws.String("8.0"),
						Options: []rdsTypes.Option{
							{
								OptionName: aws.String("MARIADB_AUDIT_PLUGIN"),
							},
						},
					},
				},
			},
			nil,
		},
		dbEngineVersions: []rdsTypes.DBEngineVersion{
			{MajorEngineVersion: aws.String("8.4")},
		},
	}

	o := newTestOptionGroupClient(mockRDS)

	i := &RDSInstance{
		Database:        "db1",
		DbType:          "mysql",
		DbVersion:       "8.4.9",
		OptionGroupName: "my-audit-group",
	}

	created, err := o.ProvisionOrModifyCustomOptionGroup(i, nil)
	if err != nil {
		t.Fatalf("unexepcted error: %s", err)
	}
	if !created {
		t.Errorf("expected a new option group to be created")
	}

	expectedName := "cg-aws-broker-db1-option-8-4"
	if i.OptionGroupName != expectedName {
		t.Errorf("expected OptionGroupName %q, got %q", expectedName, i.OptionGroupName)
	}
	if mockRDS.createOptionGroupInput == nil {
		t.Fatalf("expected CreateOptionGroup to be called")
	}
	if got := aws.ToString(mockRDS.createOptionGroupInput.EngineName); got != "mysql" {
		t.Errorf("expected EngineName mysql, got %q", got)
	}
	if got := aws.ToString(mockRDS.createOptionGroupInput.OptionGroupName); got != expectedName {
		t.Errorf("expected created group name %q, got %q", expectedName, got)
	}
	if mockRDS.modifyOptionGroupInput == nil {
		t.Fatal("expected ModifyOptionGroup to be called to copy options")
	}
	optionNames := []string{}
	for _, opt := range mockRDS.modifyOptionGroupInput.OptionsToInclude {
		optionNames = append(optionNames, aws.ToString(opt.OptionName))
	}
	if !slices.Contains(optionNames, "MARIADB_AUDIT_PLUGIN") {
		t.Errorf("expected MARIADB_AUDIT_PLUGIN to be carried forward, got %v", optionNames)
	}
}

func TestProvisionOrModifyCustomOptionGroup_NoOp(t *testing.T) {
	testCases := map[string]struct {
		instance                *RDSInstance
		describeResults         []*rds.DescribeOptionGroupsOutput
		dbEngineVersions        []rdsTypes.DBEngineVersion
		expectedOptionGroupName string
	}{
		"no option group": {
			instance:                &RDSInstance{Database: "db1", DbType: "mysql", DbVersion: "8.4.9"},
			expectedOptionGroupName: "",
		},
		"default option group": {
			instance:                &RDSInstance{Database: "db1", DbType: "mysql", DbVersion: "8.4.9", OptionGroupName: "default:mysql-8-4"},
			expectedOptionGroupName: "default:mysql-8-4",
		},
		"same major version": {
			instance: &RDSInstance{Database: "db1", DbType: "mysql", DbVersion: "8.0.46", OptionGroupName: "my-audit-group"},
			describeResults: []*rds.DescribeOptionGroupsOutput{
				{
					OptionGroupsList: []rdsTypes.OptionGroup{
						{
							OptionGroupName:    aws.String("my-audit-group"),
							MajorEngineVersion: aws.String("8.0"),
							Options: []rdsTypes.Option{
								{OptionName: aws.String("MARIADB_AUDIT_PLUGIN")},
							},
						},
					},
				},
			},
			dbEngineVersions:        []rdsTypes.DBEngineVersion{{MajorEngineVersion: aws.String("8.0")}},
			expectedOptionGroupName: "my-audit-group",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			mockRDS := &mockRDSClient{
				describeOptionGroupsResults: tc.describeResults,
				dbEngineVersions:            tc.dbEngineVersions,
			}
			o := newTestOptionGroupClient(mockRDS)

			created, err := o.ProvisionOrModifyCustomOptionGroup(tc.instance, nil)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if created {
				t.Error("expected no option group to be created")
			}
			if mockRDS.createOptionGroupInput != nil {
				t.Error("expected CreateOptionGroup not to be called")
			}
			if tc.instance.OptionGroupName != tc.expectedOptionGroupName {
				t.Errorf("expected option group %q, got %q", tc.expectedOptionGroupName, tc.instance.OptionGroupName)
			}
		})
	}
}

func TestDeleteOptionGroup(t *testing.T) {
	testCases := map[string]struct {
		optionGroupName     string
		describeResults     []*rds.DescribeOptionGroupsOutput
		expectedDeleteCalls int
	}{
		"manually-created group is never deleted": {
			optionGroupName:     "my-audit-group",
			expectedDeleteCalls: 0,
		},
		"broker group is deleted": {
			optionGroupName: "cg-aws-broker-db1-option-8-0",
			describeResults: []*rds.DescribeOptionGroupsOutput{
				{
					OptionGroupsList: []rdsTypes.OptionGroup{
						{OptionGroupName: aws.String("cg-aws-broker-db1-option-8-0")},
					},
				},
			},
			expectedDeleteCalls: 1,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			mockRDS := &mockRDSClient{describeOptionGroupsResults: tc.describeResults}
			o := newTestOptionGroupClient(mockRDS)

			if err := o.DeleteOptionGroup(tc.optionGroupName, nil); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if mockRDS.deleteOptionGroupCallNum != tc.expectedDeleteCalls {
				t.Errorf("expected DeleteOptionGroup to be called %d times(s), got %d", tc.expectedDeleteCalls, mockRDS.deleteDBInstancesCallNum)
			}
		})
	}
}

func TestDeleteOptionGroup_RetriesOnInvalidState(t *testing.T) {
	invalidState := &rdsTypes.InvalidOptionGroupStateFault{
		Message: aws.String("The option group 'cg-aws-broker-db1-option-19' cannot be deleted because it is in use."),
	}
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
		deleteOptionGroupErrs: []error{invalidState, invalidState, nil},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	if err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", nil); err != nil {
		t.Fatalf("expected the retry to eventually succeed, got error: %s", err)
	}
	if mockRDS.deleteOptionGroupCallNum != 3 {
		t.Errorf("expected DeleteOptionGroup to be called 3 times (2 failures + 1 success), got %d", mockRDS.deleteOptionGroupCallNum)
	}
}

func TestDeleteOptionGroup_OnRetryCalledForEachRetry(t *testing.T) {
	invalidState := &rdsTypes.InvalidOptionGroupStateFault{
		Message: aws.String("The option group 'cg-aws-broker-db1-option-19' cannot be deleted because it is in use."),
	}
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
		deleteOptionGroupErrs: []error{invalidState, invalidState, nil},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	var retryAttempts []int
	onRetry := func(attempt, maxAttempts int) {
		retryAttempts = append(retryAttempts, attempt)
		if maxAttempts != 5 {
			t.Errorf("expected maxAttempts to be 5, got %d", maxAttempts)
		}
	}

	if err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", onRetry); err != nil {
		t.Fatalf("expected the retry to eventually succeed, got error: %s", err)
	}
	if len(retryAttempts) != 2 {
		t.Fatalf("expected onRetry to be called exactly twice (once per failed attempt before success), got %d calls: %v", len(retryAttempts), retryAttempts)
	}
	if retryAttempts[0] != 1 || retryAttempts[1] != 2 {
		t.Errorf("expected onRetry attempt numbers [1, 2], got %v", retryAttempts)
	}
}

func TestDeleteOptionGroup_OnRetryNotCalledOnImmediateSuccess(t *testing.T) {
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	called := false
	onRetry := func(attempt, maxAttempts int) {
		called = true
	}

	if err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", onRetry); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if called {
		t.Error("expected onRetry not to be called when deletion succeeds immediately")
	}
}

func TestDeleteOptionGroup_GivesUpAfterMaxRetries(t *testing.T) {
	invalidState := &rdsTypes.InvalidOptionGroupStateFault{
		Message: aws.String("The option group 'cg-aws-broker-db1-option-19' cannot be deleted because it is in use."),
	}
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
		deleteOptionGroupErrs: []error{invalidState, invalidState, invalidState},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 2
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", nil)
	if !errors.Is(err, ErrOptionGroupStillInUse) {
		t.Fatalf("expected ErrOptionGroupStillInUse after exhausting retries on InvalidOptionGroupStateFault, got: %v", err)
	}
	if mockRDS.deleteOptionGroupCallNum != 2 {
		t.Errorf("expected DeleteOptionGroup to be called 2 times (bounded by PollAwsMaxRetries), got %d", mockRDS.deleteOptionGroupCallNum)
	}
}

func TestDeleteOptionGroup_GivesUpWithDifferentErrorOnLastAttempt(t *testing.T) {
	invalidState := &rdsTypes.InvalidOptionGroupStateFault{
		Message: aws.String("The option group 'cg-aws-broker-db1-option-19' cannot be deleted because it is in use."),
	}
	otherErr := errors.New("some other transient AWS error")
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
		deleteOptionGroupErrs: []error{invalidState, otherErr},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 2
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", nil)
	if errors.Is(err, ErrOptionGroupStillInUse) {
		t.Fatalf("expected the actual last error to be surfaced, not ErrOptionGroupStillInUse, got: %v", err)
	}
	if !errors.Is(err, otherErr) {
		t.Fatalf("expected the last error (%v) to be returned, got: %v", otherErr, err)
	}
	if mockRDS.deleteOptionGroupCallNum != 2 {
		t.Errorf("expected DeleteOptionGroup to be called 2 times (bounded by PollAwsMaxRetries), got %d", mockRDS.deleteOptionGroupCallNum)
	}
}

func TestDeleteOptionGroup_NotFoundIsSuccess(t *testing.T) {
	notFound := &rdsTypes.OptionGroupNotFoundFault{}
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
		deleteOptionGroupErrs: []error{notFound},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	if err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", nil); err != nil {
		t.Fatalf("expected OptionGroupNotFoundFault to be treated as success, got error: %s", err)
	}
	if mockRDS.deleteOptionGroupCallNum != 1 {
		t.Errorf("expected DeleteOptionGroup to be called exactly once (no retry on not-found), got %d", mockRDS.deleteOptionGroupCallNum)
	}
}

func TestDeleteOptionGroup_NonRetryableErrorFailsImmediately(t *testing.T) {
	otherErr := errors.New("boom")
	mockRDS := &mockRDSClient{
		describeOptionGroupsResults: []*rds.DescribeOptionGroupsOutput{
			{
				OptionGroupsList: []rdsTypes.OptionGroup{
					{OptionGroupName: aws.String("cg-aws-broker-db1-option-19")},
				},
			},
		},
		deleteOptionGroupErrs: []error{otherErr},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	err := o.DeleteOptionGroup("cg-aws-broker-db1-option-19", nil)
	if err == nil {
		t.Fatal("expected a non-retryable error to be returned")
	}
	if mockRDS.deleteOptionGroupCallNum != 1 {
		t.Errorf("expected DeleteOptionGroup to be called exactly once (no retry), got %d", mockRDS.deleteOptionGroupCallNum)
	}
}

func TestModifyOptionGroupWithRetry_RetriesOnInvalidState(t *testing.T) {
	invalidState := &rdsTypes.InvalidOptionGroupStateFault{
		Message: aws.String("This option group cannot be modified because it is currently being applied to DB Instance some-other-instance."),
	}
	mockRDS := &mockRDSClient{
		modifyOptionGroupErrs: []error{invalidState, invalidState, nil},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	opts := []rdsTypes.OptionConfiguration{{OptionName: aws.String("SSL")}}
	err := o.modifyOptionGroupWithRetry("cg-aws-broker-db1-option-19", opts)
	if err != nil {
		t.Fatalf("expected the retry to eventually succeed, got error: %s", err)
	}
	if mockRDS.modifyOptionGroupCallNum != 3 {
		t.Errorf("expected ModifyOptionGroup to be called 3 times (2 failures + 1 success), got %d", mockRDS.modifyOptionGroupCallNum)
	}
}

func TestModifyOptionGroupWithRetry_GivesUpAfterMaxRetries(t *testing.T) {
	invalidState := &rdsTypes.InvalidOptionGroupStateFault{
		Message: aws.String("This option group cannot be modified because it is currently being applied to DB Instance some-other-instance."),
	}
	mockRDS := &mockRDSClient{
		modifyOptionGroupErrs: []error{invalidState, invalidState, invalidState},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 2
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	opts := []rdsTypes.OptionConfiguration{{OptionName: aws.String("SSL")}}
	err := o.modifyOptionGroupWithRetry("cg-aws-broker-db1-option-19", opts)
	if err == nil {
		t.Fatal("expected an error after exhausting retries")
	}
	if mockRDS.modifyOptionGroupCallNum != 3 {
		t.Errorf("expected ModifyOptionGroup to be called 3 times (1 initial + 2 retries), got %d", mockRDS.modifyOptionGroupCallNum)
	}
}

func TestModifyOptionGroupWithRetry_NonRetryableErrorFailsImmediately(t *testing.T) {
	otherErr := &rdsTypes.OptionGroupNotFoundFault{}
	mockRDS := &mockRDSClient{
		modifyOptionGroupErrs: []error{otherErr},
	}
	o := newTestOptionGroupClient(mockRDS)
	o.settings.PollAwsMaxRetries = 5
	o.settings.PollAwsMinDelay = 1 * time.Millisecond

	opts := []rdsTypes.OptionConfiguration{{OptionName: aws.String("SSL")}}
	err := o.modifyOptionGroupWithRetry("cg-aws-broker-db1-option-19", opts)
	if err == nil {
		t.Fatal("expected a non-retryable error to be returned")
	}
	if mockRDS.modifyOptionGroupCallNum != 1 {
		t.Errorf("expected ModifyOptionGroup to be called exactly once (no retry), got %d", mockRDS.modifyOptionGroupCallNum)
	}
}
