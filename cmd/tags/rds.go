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

func getRdsInstanceTags(rdsClient rdsiface.RDSAPI, rdsInstance *rds.RDSInstance) ([]*awsRds.Tag, bool, error) {
	instanceInfo, err := rdsClient.DescribeDBInstances(&awsRds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(rdsInstance.Database),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == awsRds.ErrCodeDBInstanceNotFoundFault {
				log.Printf("Could not find database %s, continuing", rdsInstance.Database)
				return []*awsRds.Tag{}, false, nil
			} else {
				return []*awsRds.Tag{}, false, fmt.Errorf("could not describe database instance: %s", err)
			}
		} else {
			return []*awsRds.Tag{}, false, fmt.Errorf("could not describe database instance: %s", err)
		}
	}

	tagsResponse, err := rdsClient.ListTagsForResource(&awsRds.ListTagsForResourceInput{
		ResourceName: instanceInfo.DBInstances[0].DBInstanceArn,
	})
	if err != nil {
		log.Fatalf("error getting tags for database %s: %s", rdsInstance.Database, err)
	}

	return tagsResponse.TagList, true, nil
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

func doRDSTagsContainGeneratedTags(rdsTags []*awsRds.Tag, generatedTags []*awsRds.Tag) bool {
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

func fetchAndUpdateRdsInstanceTags(catalog *catalog.Catalog, db *gorm.DB, rdsClient rdsiface.RDSAPI, tagManager brokertags.TagManager) {
	rows, err := db.Model(&rds.RDSInstance{}).Rows()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var rdsInstance rds.RDSInstance
		db.ScanRows(rows, &rdsInstance)

		existingRdsTags, shouldContinue, err := getRdsInstanceTags(rdsClient, &rdsInstance)
		if err != nil {
			log.Fatalf("could not get tags for database %s: %s", rdsInstance.Database, err)
		}
		if !shouldContinue {
			continue
		}

		plan, _ := catalog.RdsService.FetchPlan(rdsInstance.PlanID)
		if plan.Name == "" {
			log.Fatalf("error getting plan %s for database %s", rdsInstance.PlanID, rdsInstance.Database)
		}

		generatedTags, err := tagManager.GenerateTags(
			brokertags.Create,
			catalog.RdsService.Name,
			plan.Name,
			brokertags.ResourceGUIDs{
				InstanceGUID:     rdsInstance.Uuid,
				SpaceGUID:        rdsInstance.SpaceGUID,
				OrganizationGUID: rdsInstance.OrganizationGUID,
			},
			true,
		)
		if err != nil {
			log.Fatalf("error generating new tags for database %s: %s", rdsInstance.Database, err)
		}

		generatedRdsTags := convertTagsToRDSTags(generatedTags)
		if doRDSTagsContainGeneratedTags(existingRdsTags, generatedRdsTags) {
			log.Printf("tags already updated for database %s", rdsInstance.Database)
			continue
		}

		log.Printf("need to update tags for database %s", rdsInstance.Database)
	}
}
