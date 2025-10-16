package logs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

type CloudwatchLogClientsInterface interface {
	DescribeLogGroups(ctx context.Context, params *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error)
	TagResource(ctx context.Context, params *cloudwatchlogs.TagResourceInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.TagResourceOutput, error)
}
