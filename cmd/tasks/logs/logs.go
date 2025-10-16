package logs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

func DescribeLogGroups(logsClient CloudwatchLogClientsInterface, logGroupNamePrefix string) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	return logsClient.DescribeLogGroups(context.TODO(), &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(logGroupNamePrefix),
	})
}
