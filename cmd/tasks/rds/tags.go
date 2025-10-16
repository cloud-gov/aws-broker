package rds

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsRds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/services/rds"
	brokertags "github.com/cloud-gov/go-broker-tags"

	"github.com/cloud-gov/aws-broker/cmd/tasks/logs"
	"github.com/cloud-gov/aws-broker/cmd/tasks/tags"

	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

func getRDSResourceTags(rdsClient RDSClientInterface, dbInstanceArn string) ([]rdsTypes.Tag, error) {
	tagsResponse, err := rdsClient.ListTagsForResource(&awsRds.ListTagsForResourceInput{
		ResourceName: aws.String(dbInstanceArn),
	})
	if err != nil {
		return []rdsTypes.Tag{}, fmt.Errorf("error getting tags for database %s: %s", dbInstanceArn, err)
	}

	return tagsResponse.TagList, nil
}

func getRDSInstanceArn(rdsClient RDSClientInterface, database string) (string, error) {
	instanceInfo, err := rdsClient.DescribeDBInstances(context.TODO(), &awsRds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	})

	if err != nil {
		var notFoundException *rdsTypes.DBInstanceNotFoundFault
		if errors.As(err, &notFoundException) {
			log.Printf("Could not find database %s, continuing", database)
			return "", nil
		} else {
			return "", fmt.Errorf("could not describe database instance: %s", err)
		}
	}

	return *instanceInfo.DBInstances[0].DBInstanceArn, nil
}

func doRDSResourceTagsContainGeneratedTags(rdsTags []rdsTypes.Tag, generatedTags []rdsTypes.Tag) bool {
	for _, v := range generatedTags {
		if slices.Contains([]string{"Created at", "Updated at"}, *v.Key) {
			continue
		}

		if !slices.ContainsFunc(rdsTags, func(tag rdsTypes.Tag) bool {
			return *tag.Key == *v.Key && *tag.Value == *v.Value
		}) {
			return false
		}
	}
	return true
}

func applyTagsToRDSResource(rdsClient RDSClientInterface, instanceArn string, tags []rdsTypes.Tag) error {
	_, err := rdsClient.AddTagsToResource(context.TODO(), &awsRds.AddTagsToResourceInput{
		ResourceName: aws.String(instanceArn),
		Tags:         tags,
	})
	return err
}

func reconcileRDSResourceTags(rdsClient RDSClientInterface, instanceArn string, generatedTags []rdsTypes.Tag) error {
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

func reconcileRDSParameterGroupTags(rdsInstance rds.RDSInstance, generatedRdsTags []rdsTypes.Tag, rdsClient RDSClientInterface) error {
	groupInfo, err := rdsClient.DescribeDBParameterGroups(context.TODO(), &awsRds.DescribeDBParameterGroupsInput{
		DBParameterGroupName: aws.String(rdsInstance.ParameterGroupName),
	})
	if err != nil {
		return fmt.Errorf("could not find parameter group with name %s: %s", rdsInstance.ParameterGroupName, err)
	}

	if len(groupInfo.DBParameterGroups) == 0 {
		return fmt.Errorf("could not find parameter group with name %s", rdsInstance.ParameterGroupName)
	}

	parameterGroupArn := groupInfo.DBParameterGroups[0].DBParameterGroupArn

	err = reconcileRDSResourceTags(rdsClient, *parameterGroupArn, generatedRdsTags)
	if err != nil {
		return fmt.Errorf("could not process tags for parameter group with name %s: %s", rdsInstance.ParameterGroupName, err)
	}

	return nil
}

func reconcileRDSInstanceTags(rdsClient RDSClientInterface, database string, generatedRdsTags []rdsTypes.Tag) error {
	dbInstanceArn, err := getRDSInstanceArn(rdsClient, database)
	if err != nil {
		return fmt.Errorf("could not get ARN for database %s: %s", database, err)
	}
	if dbInstanceArn == "" {
		return nil
	}
	err = reconcileRDSResourceTags(rdsClient, dbInstanceArn, generatedRdsTags)
	if err != nil {
		return fmt.Errorf("failed to process database %s: %s", database, err)
	}

	return nil
}

func reconcileResourceTagsForRDSDatabase(rdsInstance rds.RDSInstance, catalog *catalog.Catalog, rdsClient RDSClientInterface, logsClient logs.CloudwatchLogClientsInterface, tagManager brokertags.TagManager) error {
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

	err = reconcileRDSInstanceTags(rdsClient, rdsInstance.Database, generatedRdsTags)
	if err != nil {
		return fmt.Errorf("failed to reconcile tags for database %s: %s", rdsInstance.Database, err)
	}

	if rdsInstance.ReplicaDatabase != "" {
		err = reconcileRDSInstanceTags(rdsClient, rdsInstance.ReplicaDatabase, generatedRdsTags)
		if err != nil {
			return fmt.Errorf("failed to reconcile tags for database %s: %s", rdsInstance.ReplicaDatabase, err)
		}
	}

	if rdsInstance.ParameterGroupName != "" {
		err := reconcileRDSParameterGroupTags(rdsInstance, generatedRdsTags, rdsClient)
		if err != nil {
			return err
		}
	}

	if len(rdsInstance.EnabledCloudwatchLogGroupExports) == 0 {
		log.Printf("no enabled log groups for database %s", rdsInstance.Database)
		return nil
	}

	for _, logGroupType := range rdsInstance.EnabledCloudwatchLogGroupExports {
		logGroupName := getLogGroupPrefix(rdsInstance.Database, logGroupType)

		err = logs.TagCloudwatchLogGroup(logGroupName, generatedTags, logsClient)
		if err != nil {
			return fmt.Errorf("could not apply tags to cloudwatch log group %s: %s", logGroupName, err)
		}
	}

	return nil
}

func ReconcileResourceTagsForAllRDSDatabases(catalog *catalog.Catalog, db *gorm.DB, rdsClient RDSClientInterface, logsClient logs.CloudwatchLogClientsInterface, tagManager brokertags.TagManager) error {
	rows, err := db.Model(&rds.RDSInstance{}).Rows()
	if err != nil {
		return err
	}

	var errs error

	for rows.Next() {
		var rdsInstance rds.RDSInstance
		db.ScanRows(rows, &rdsInstance)

		err := reconcileResourceTagsForRDSDatabase(rdsInstance, catalog, rdsClient, logsClient, tagManager)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
	}

	return errs
}
