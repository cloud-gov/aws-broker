package logs

import (
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

func TagCloudwatchLogGroup(logGroupName string, generatedTags map[string]string, logsClient cloudwatchlogsiface.CloudWatchLogsAPI) error {
	log.Printf("adding tags to log group %s", logGroupName)

	resp, err := DescribeLogGroups(logsClient, logGroupName)
	if err != nil {
		return err
	}

	if len(resp.LogGroups) == 0 {
		log.Printf("could not find log group %s", logGroupName)
		return nil
	}

	logGroupArn := *resp.LogGroups[0].Arn
	logGroupArn, _ = strings.CutSuffix(logGroupArn, ":*")

	cloudwatchTags := make(map[string]*string)
	for key, value := range generatedTags {
		cloudwatchTags[key] = aws.String(value)
	}

	_, err = logsClient.TagResource(&cloudwatchlogs.TagResourceInput{
		ResourceArn: aws.String(logGroupArn),
		Tags:        cloudwatchTags,
	})
	if err != nil {
		return err
	}

	log.Printf("finished updating tags for log group %s", logGroupName)
	return nil
}
