package logs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

func TagCloudwatchLogGroup(logsClient cloudwatchlogsiface.CloudWatchLogsAPI, logGroupArn string) error {
	_, err := logsClient.TagResource(&cloudwatchlogs.TagResourceInput{
		ResourceArn: aws.String(logGroupArn),
	})
	return err
}
