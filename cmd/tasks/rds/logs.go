package rds

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/services/rds"
	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/cmd/tasks/logs"
)

func getLogGroupPrefix(prefixParts ...string) string {
	return fmt.Sprintf("/aws/rds/instance/%s", strings.Join(prefixParts, "/"))
}

func ReconcileRDSCloudwatchLogGroups(logsClient cloudwatchlogsiface.CloudWatchLogsAPI, rdsClient rdsiface.RDSAPI, dbNamePrefix string, db *gorm.DB) error {
	resp, err := logs.DescribeLogGroups(logsClient, getLogGroupPrefix(dbNamePrefix))
	if err != nil {
		return err
	}

	for _, logGroup := range resp.LogGroups {
		log.Printf("found group: %s", *logGroup.LogGroupName)
		res := strings.Split(*logGroup.LogGroupName, "/")
		if len(res) < 4 {
			return fmt.Errorf("error parsing log group name %s", *logGroup.LogGroupName)
		}

		dbName := res[4]
		if dbName == "" {
			return fmt.Errorf("could not get database name for log group %s", *logGroup.LogGroupName)
		}
		log.Printf("got database name %s from group %s", dbName, *logGroup.LogGroupName)

		var rdsDatabase rds.RDSInstance
		err := db.Where(&rds.RDSInstance{Database: dbName}).First(&rdsDatabase).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Printf("could not find database record with name %s, continuing", dbName)
				continue
			} else {
				return err
			}
		}

		resp, err := rdsClient.DescribeDBInstances(&awsRds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(dbName),
		})
		if err != nil {
			return err
		}

		instanceInfo := resp.DBInstances[0]

		var enabledGroups []string
		for _, enabledGroup := range instanceInfo.EnabledCloudwatchLogsExports {
			enabledGroups = append(enabledGroups, *enabledGroup)
		}

		log.Printf("database name %s has log groups enabled %v", dbName, enabledGroups)

		rdsDatabase.EnabledCloudwatchLogGroupExports = enabledGroups
		err = db.Save(&rdsDatabase).Error
		if err != nil {
			return err
		}

		log.Printf("saved enabled log groups for %s", dbName)
	}

	return nil
}
