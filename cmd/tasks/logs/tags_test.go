package logs

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cloudwatchTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

type mockLogsClient struct {
	logGroups            []cloudwatchTypes.LogGroup
	describeLogGroupsErr error
	tagResourceErr       error
}

func (m mockLogsClient) DescribeLogGroups(ctx context.Context, params *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	if m.describeLogGroupsErr != nil {
		return nil, m.describeLogGroupsErr
	}
	return &cloudwatchlogs.DescribeLogGroupsOutput{
		LogGroups: m.logGroups,
	}, nil
}

func (m mockLogsClient) TagResource(ctx context.Context, params *cloudwatchlogs.TagResourceInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.TagResourceOutput, error) {
	return nil, m.tagResourceErr
}

func TestTagCloudwatchLogGroup(t *testing.T) {
	testCases := map[string]struct {
		logGroupName   string
		generatedTags  map[string]string
		expectErr      bool
		mockLogsClient *mockLogsClient
	}{
		"success": {
			generatedTags: map[string]string{
				"foo": "bar",
			},
			mockLogsClient: &mockLogsClient{
				logGroups: []cloudwatchTypes.LogGroup{
					{
						Arn: aws.String("group1-arn"),
					},
				},
			},
		},
		"error describing log group": {
			mockLogsClient: &mockLogsClient{
				describeLogGroupsErr: errors.New("error describing log group"),
			},
			expectErr: true,
		},
		"no error, but log group not found": {
			mockLogsClient: &mockLogsClient{
				logGroups: []cloudwatchTypes.LogGroup{},
			},
		},
		"error tagging log group": {
			generatedTags: map[string]string{
				"foo": "bar",
			},
			mockLogsClient: &mockLogsClient{
				logGroups: []cloudwatchTypes.LogGroup{
					{
						Arn: aws.String("group1-arn"),
					},
				},
				tagResourceErr: errors.New("error tagging resource"),
			},
			expectErr: true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := TagCloudwatchLogGroup(test.logGroupName, test.generatedTags, test.mockLogsClient)
			if err != nil && !test.expectErr {
				t.Error(err)
			}
			if err == nil && test.expectErr {
				t.Error("expected error, but received nil")
			}
		})
	}
}
