package rds

import (
	"fmt"
	"log"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/services/rds"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	brokertags "github.com/cloud-gov/go-broker-tags"

	"github.com/cloud-gov/aws-broker/cmd/tasks/logs"
	"github.com/cloud-gov/aws-broker/cmd/tasks/tags"

	"github.com/jinzhu/gorm"
	"golang.org/x/exp/slices"
)

func getRDSResourceTags(rdsClient rdsiface.RDSAPI, dbInstanceArn string) ([]*awsRds.Tag, error) {
	tagsResponse, err := rdsClient.ListTagsForResource(&awsRds.ListTagsForResourceInput{
		ResourceName: aws.String(dbInstanceArn),
	})
	if err != nil {
		return []*awsRds.Tag{}, fmt.Errorf("error getting tags for database %s: %s", dbInstanceArn, err)
	}

	return tagsResponse.TagList, nil
}

func getRDSInstanceArn(rdsClient rdsiface.RDSAPI, rdsInstance rds.RDSInstance) (string, error) {
	instanceInfo, err := rdsClient.DescribeDBInstances(&awsRds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(rdsInstance.Database),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == awsRds.ErrCodeDBInstanceNotFoundFault {
				log.Printf("Could not find database %s, continuing", rdsInstance.Database)
				return "", nil
			} else {
				return "", fmt.Errorf("could not describe database instance: %s", err)
			}
		} else {
			return "", fmt.Errorf("could not describe database instance: %s", err)
		}
	}

	return *instanceInfo.DBInstances[0].DBInstanceArn, nil
}

func doRDSResourceTagsContainGeneratedTags(rdsTags []*awsRds.Tag, generatedTags []*awsRds.Tag) bool {
	for _, v := range generatedTags {
		if slices.Contains([]string{"Created at", "Updated at"}, *v.Key) {
			continue
		}

		if !slices.ContainsFunc(rdsTags, func(tag *awsRds.Tag) bool {
			return *tag.Key == *v.Key && *tag.Value == *v.Value
		}) {
			return false
		}
	}
	return true
}

func applyTagsToRDSResource(rdsClient rdsiface.RDSAPI, instanceArn string, tags []*awsRds.Tag) error {
	_, err := rdsClient.AddTagsToResource(&awsRds.AddTagsToResourceInput{
		ResourceName: aws.String(instanceArn),
		Tags:         tags,
	})
	return err
}

func processRDSResource(rdsClient rdsiface.RDSAPI, instanceArn string, generatedTags []*awsRds.Tag) error {
	existingTags, err := getRDSResourceTags(rdsClient, instanceArn)
	if err != nil {
		return fmt.Errorf("could not find resource %s: %s", instanceArn, err)
	}

	if doRDSResourceTagsContainGeneratedTags(existingTags, generatedTags) {
		log.Printf("tags already updated for resource %s", instanceArn)
		return nil
	}

	log.Printf("updating tags for resource %s", instanceArn)
	err = applyTagsToRDSResource(rdsClient, instanceArn, generatedTags)
	if err != nil {
		return fmt.Errorf("error adding new tags for resource %s: %s", instanceArn, err)
	}

	log.Printf("finished updating tags for resource %s", instanceArn)
	return nil
}

func ReconcileRDSResourceTags(catalog *catalog.Catalog, db *gorm.DB, rdsClient rdsiface.RDSAPI, logsClient cloudwatchlogsiface.CloudWatchLogsAPI, tagManager brokertags.TagManager) error {
	rows, err := db.Model(&rds.RDSInstance{}).Rows()
	if err != nil {
		return err
	}

	for rows.Next() {
		var rdsInstance rds.RDSInstance
		db.ScanRows(rows, &rdsInstance)

		dbInstanceArn, err := getRDSInstanceArn(rdsClient, rdsInstance)
		if err != nil {
			return fmt.Errorf("could not get ARN for database %s: %s", rdsInstance.Database, err)
		}
		if dbInstanceArn == "" {
			continue
		}

		plan, _ := catalog.RdsService.FetchPlan(rdsInstance.PlanID)
		if plan.Name == "" {
			return fmt.Errorf("error getting plan %s for database %s", rdsInstance.PlanID, rdsInstance.Database)
		}

		generatedTags, err := tags.GenerateTags(
			tagManager,
			catalog.RdsService.Name,
			plan.Name,
			brokertags.ResourceGUIDs{
				InstanceGUID:     rdsInstance.Uuid,
				SpaceGUID:        rdsInstance.SpaceGUID,
				OrganizationGUID: rdsInstance.OrganizationGUID,
			},
		)
		if err != nil {
			return fmt.Errorf("error generating new tags for database %s: %s", rdsInstance.Database, err)
		}

		generatedRdsTags := rds.ConvertTagsToRDSTags(generatedTags)

		err = processRDSResource(rdsClient, dbInstanceArn, generatedRdsTags)
		if err != nil {
			return err
		}

		if rdsInstance.ParameterGroupName != "" {
			groupInfo, err := rdsClient.DescribeDBParameterGroups(&awsRds.DescribeDBParameterGroupsInput{
				DBParameterGroupName: aws.String(rdsInstance.ParameterGroupName),
			})
			if err != nil {
				log.Fatalf("could not find parameter group with name %s: %s", rdsInstance.ParameterGroupName, err)
			}

			parameterGroupArn := groupInfo.DBParameterGroups[0].DBParameterGroupArn

			err = processRDSResource(rdsClient, *parameterGroupArn, generatedRdsTags)
			if err != nil {
				return err
			}
		}

		if len(rdsInstance.EnabledCloudwatchLogGroupExports) == 0 {
			log.Printf("no enabled log groups for database %s", rdsInstance.Database)
			continue
		}

		for _, logGroupType := range rdsInstance.EnabledCloudwatchLogGroupExports {
			logGroupName := getLogGroupPrefix(rdsInstance.Database, logGroupType)

			err = logs.TagCloudwatchLogGroup(logGroupName, generatedTags, logsClient)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
