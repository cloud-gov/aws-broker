package logs

import (
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/jinzhu/gorm"
)

func ReconcileRDSCloudwatchLogGroups(logsClient cloudwatchlogsiface.CloudWatchLogsAPI, rdsClient rdsiface.RDSAPI, dbNamePrefix string, db *gorm.DB) error {
	resp, err := logsClient.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(fmt.Sprintf("/aws/rds/instance/%s", dbNamePrefix)),
	})
	if err != nil {
		return err
	}

	for _, logGroup := range resp.LogGroups {
		log.Printf("found group: %s", *logGroup.LogGroupName)
		res := strings.Split("/", *logGroup.LogGroupName)
		dbName := res[3]
		if dbName == "" {
			return fmt.Errorf("could not get database name for log group %s", *logGroup.LogGroupName)
		}
		log.Printf("got database name %s from group %s", dbName, logGroup)

		// var rdsDatabase rds.RDSInstance
		// db.Where(&rds.RDSInstance{Database: dbName}).First(&rdsDatabase)
		// if *rdsDatabase == nil {
		// 	log.Printf("could not find database with name %s", dbName)
		// 	continue
		// }

		// dbInfo, err := rdsClient.DescribeDBInstances(&rds.DescribeDBInstancesInput{
		// 	DBInstanceIdentifier: aws.String(dbName),
		// })
		// if err != nil {
		// 	return err
		// }
	}

	return nil
}
