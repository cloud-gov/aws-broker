package logs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

func DescribeLogGroups(logsClient cloudwatchlogsiface.CloudWatchLogsAPI, logGroupNamePrefix string) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	return logsClient.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(logGroupNamePrefix),
	})
}
