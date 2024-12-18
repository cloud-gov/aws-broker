package main

import (
	"fmt"
	"log"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/services/rds"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/jinzhu/gorm"
	"golang.org/x/exp/slices"
)

func getRdsResourceTags(rdsClient rdsiface.RDSAPI, dbInstanceArn string) ([]*awsRds.Tag, error) {
	tagsResponse, err := rdsClient.ListTagsForResource(&awsRds.ListTagsForResourceInput{
		ResourceName: aws.String(dbInstanceArn),
	})
	if err != nil {
		return []*awsRds.Tag{}, fmt.Errorf("error getting tags for database %s: %s", dbInstanceArn, err)
	}

	return tagsResponse.TagList, nil
}

func getRdsInstanceArn(rdsClient rdsiface.RDSAPI, rdsInstance rds.RDSInstance) (string, error) {
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

func convertTagsToRDSTags(tags map[string]string) []*awsRds.Tag {
	var rdsTags []*awsRds.Tag
	for k, v := range tags {
		tag := awsRds.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		rdsTags = append(rdsTags, &tag)
	}
	return rdsTags
}

func doExistingTagsContainGeneratedTags(rdsTags []*awsRds.Tag, generatedTags []*awsRds.Tag) bool {
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

func fetchAndUpdateRdsInstanceTags(catalog *catalog.Catalog, db *gorm.DB, rdsClient rdsiface.RDSAPI, tagManager brokertags.TagManager) error {
	rows, err := db.Model(&rds.RDSInstance{}).Rows()
	if err != nil {
		return err
	}

	for rows.Next() {
		var rdsInstance rds.RDSInstance
		db.ScanRows(rows, &rdsInstance)

		dbInstanceArn, err := getRdsInstanceArn(rdsClient, rdsInstance)
		if err != nil {
			return fmt.Errorf("could not get ARN for database %s: %s", rdsInstance.Database, err)
		}
		if dbInstanceArn == "" {
			continue
		}

		existingRdsTags, err := getRdsResourceTags(rdsClient, dbInstanceArn)
		if err != nil {
			return fmt.Errorf("could not get tags for database %s: %s", rdsInstance.Database, err)
		}

		plan, _ := catalog.RdsService.FetchPlan(rdsInstance.PlanID)
		if plan.Name == "" {
			return fmt.Errorf("error getting plan %s for database %s", rdsInstance.PlanID, rdsInstance.Database)
		}

		generatedTags, err := generateTags(
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

		generatedRdsTags := convertTagsToRDSTags(generatedTags)
		if doExistingTagsContainGeneratedTags(existingRdsTags, generatedRdsTags) {
			log.Printf("tags already updated for database %s", rdsInstance.Database)
			continue
		}

		log.Printf("updating tags for database %s", rdsInstance.Database)
		err = applyTagsToRDSResource(rdsClient, dbInstanceArn, generatedRdsTags)
		if err != nil {
			return fmt.Errorf("error adding new tags for database %s: %s", rdsInstance.Database, err)
		}

		log.Printf("finished updating tags for database %s", rdsInstance.Database)

		if rdsInstance.ParameterGroupName != "" {
			groupInfo, err := rdsClient.DescribeDBParameterGroups(&awsRds.DescribeDBParameterGroupsInput{
				DBParameterGroupName: aws.String(rdsInstance.ParameterGroupName),
			})
			if err != nil {
				log.Fatalf("could not find parameter group with name %s: %s", rdsInstance.ParameterGroupName, err)
			}

			parameterGroupArn := groupInfo.DBParameterGroups[0].DBParameterGroupArn

			existingGroupTags, err := getRdsResourceTags(rdsClient, *parameterGroupArn)
			if err != nil {
				log.Fatalf("could not find parameter group with name %s: %s", rdsInstance.ParameterGroupName, err)
			}

			if doExistingTagsContainGeneratedTags(existingGroupTags, generatedRdsTags) {
				log.Printf("tags already updated for group %s", rdsInstance.ParameterGroupName)
				continue
			}

			log.Printf("updating tags for group %s", rdsInstance.ParameterGroupName)
			err = applyTagsToRDSResource(rdsClient, dbInstanceArn, generatedRdsTags)
			if err != nil {
				log.Fatalf("error adding new tags for group %s: %s", rdsInstance.ParameterGroupName, err)
			}

			log.Printf("finished updating tags for group %s", rdsInstance.ParameterGroupName)
		}
	}

	return nil
}
