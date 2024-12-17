package main

import (
	"fmt"
	"log"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/services/redis"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/elasticache"
	"github.com/aws/aws-sdk-go/service/elasticache/elasticacheiface"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/jinzhu/gorm"
	"golang.org/x/exp/slices"
)

func getElasticacheInstanceArn(elasticacheClient elasticacheiface.ElastiCacheAPI, redisInstance redis.RedisInstance) (string, error) {
	instanceInfo, err := elasticacheClient.DescribeReplicationGroups(&elasticache.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String(redisInstance.ClusterID),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == elasticache.ErrCodeReplicationGroupNotFoundFault {
				log.Printf("Could not find cluster %s, continuing", redisInstance.ClusterID)
				return "", nil
			} else {
				return "", fmt.Errorf("could not describe cluster: %s", err)
			}
		} else {
			return "", fmt.Errorf("could not describe cluster: %s", err)
		}
	}

	return *instanceInfo.ReplicationGroups[0].ARN, nil
}

func getElasticacheInstanceTags(elasticacheClient elasticacheiface.ElastiCacheAPI, instanceArn string) ([]*elasticache.Tag, error) {
	response, err := elasticacheClient.ListTagsForResource(&elasticache.ListTagsForResourceInput{
		ResourceName: aws.String(instanceArn),
	})
	if err != nil {
		return []*elasticache.Tag{}, fmt.Errorf("error getting tags for cluster %s: %s", instanceArn, err)
	}
	return response.TagList, nil
}

func convertTagsToElasticacheTags(tags map[string]string) []*elasticache.Tag {
	var elasticacheTags []*elasticache.Tag
	for k, v := range tags {
		tag := elasticache.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		elasticacheTags = append(elasticacheTags, &tag)
	}
	return elasticacheTags
}

func doInstanceTagsContainGeneratedTags(rdsTags []*elasticache.Tag, generatedTags []*elasticache.Tag) bool {
	for _, v := range generatedTags {
		if slices.Contains([]string{"Created at", "Updated at"}, *v.Key) {
			continue
		}

		if !slices.ContainsFunc(rdsTags, func(tag *elasticache.Tag) bool {
			return *tag.Key == *v.Key && *tag.Value == *v.Value
		}) {
			return false
		}
	}
	return true
}

func fetchAndUpdateElasticacheInstanceTags(catalog *catalog.Catalog, db *gorm.DB, elasticacheClient elasticacheiface.ElastiCacheAPI, tagManager brokertags.TagManager) {
	rows, err := db.Model(&redis.RedisInstance{}).Rows()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var redisInstance redis.RedisInstance
		db.ScanRows(rows, &redisInstance)

		instanceArn, err := getElasticacheInstanceArn(elasticacheClient, redisInstance)
		if err != nil {
			log.Fatalf("could not get ARN for cluster %s: %s", redisInstance.ClusterID, err)
		}
		if instanceArn == "" {
			continue
		}

		existingInstanceTags, err := getElasticacheInstanceTags(elasticacheClient, instanceArn)
		if err != nil {
			log.Fatalf("could not get tags for cluster %s: %s", redisInstance.ClusterID, err)
		}

		plan, _ := catalog.RedisService.FetchPlan(redisInstance.PlanID)
		if plan.Name == "" {
			log.Fatalf("error getting plan %s for cluster %s", redisInstance.PlanID, redisInstance.ClusterID)
		}

		generatedTags, err := generateTags(
			tagManager,
			catalog.RdsService.Name,
			plan.Name,
			brokertags.ResourceGUIDs{
				InstanceGUID:     redisInstance.Uuid,
				SpaceGUID:        redisInstance.SpaceGUID,
				OrganizationGUID: redisInstance.OrganizationGUID,
			},
		)
		if err != nil {
			log.Fatalf("error generating new tags for cluster %s: %s", redisInstance.ClusterID, err)
		}

		generatedElasticacheTags := convertTagsToElasticacheTags(generatedTags)
		if doInstanceTagsContainGeneratedTags(existingInstanceTags, generatedElasticacheTags) {
			log.Printf("tags already updated for cluster %s", redisInstance.ClusterID)
			continue
		}

		log.Printf("updating tags for cluster %s", redisInstance.ClusterID)
		// _, err = elasticacheClient.AddTagsToResource(&elasticache.AddTagsToResourceInput{
		// 	ResourceName: aws.String(instanceArn),
		// 	Tags:         generatedElasticacheTags,
		// })
		// if err != nil {
		// 	log.Fatalf("error adding new tags for cluster %s: %s", redisInstance.ClusterID, err)
		// }

		// log.Printf("finished updating tags for cluster %s", redisInstance.ClusterID)
	}
}
