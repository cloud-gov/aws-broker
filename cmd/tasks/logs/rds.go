package logs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

func ReconcileRDSCloudwatchLogGroups(logsClient cloudwatchlogsiface.CloudWatchLogsAPI, dbNamePrefix string) error {
	resp, err := logsClient.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(fmt.Sprintf("/aws/rds/%s", dbNamePrefix)),
	})
	if err != nil {
		return err
	}

	for _, logGroup := range resp.LogGroups {
		fmt.Printf("found group: %s", logGroup)
	}

	return nil
}
