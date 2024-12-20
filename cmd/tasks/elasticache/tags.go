package elasticache

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

	"github.com/cloud-gov/aws-broker/cmd/tasks/tags"
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

func getElasticacheResourceTags(elasticacheClient elasticacheiface.ElastiCacheAPI, instanceArn string) ([]*elasticache.Tag, error) {
	response, err := elasticacheClient.ListTagsForResource(&elasticache.ListTagsForResourceInput{
		ResourceName: aws.String(instanceArn),
	})
	if err != nil {
		return []*elasticache.Tag{}, fmt.Errorf("error getting tags for cluster %s: %s", instanceArn, err)
	}
	return response.TagList, nil
}

func doElasticacheResourceTagsContainGeneratedTags(rdsTags []*elasticache.Tag, generatedTags []*elasticache.Tag) bool {
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

func processElasticacheResource(elasticacheClient elasticacheiface.ElastiCacheAPI, resourceArn string, generatedTags []*elasticache.Tag) error {
	existingTags, err := getElasticacheResourceTags(elasticacheClient, resourceArn)
	if err != nil {
		return fmt.Errorf("could not get tags for resource %s: %s", resourceArn, err)
	}

	if doElasticacheResourceTagsContainGeneratedTags(existingTags, generatedTags) {
		log.Printf("tags already updated for resource %s", resourceArn)
		return nil
	}

	log.Printf("updating tags for resource %s", resourceArn)
	_, err = elasticacheClient.AddTagsToResource(&elasticache.AddTagsToResourceInput{
		ResourceName: aws.String(resourceArn),
		Tags:         generatedTags,
	})
	if err != nil {
		return fmt.Errorf("error adding new tags for resource %s: %s", resourceArn, err)
	}

	log.Printf("finished updating tags for resource %s", resourceArn)
	return nil
}

func ReconcileElasticacheResourceTags(catalog *catalog.Catalog, db *gorm.DB, elasticacheClient elasticacheiface.ElastiCacheAPI, tagManager brokertags.TagManager) error {
	rows, err := db.Model(&redis.RedisInstance{}).Rows()
	if err != nil {
		return err
	}

	for rows.Next() {
		var redisInstance redis.RedisInstance
		db.ScanRows(rows, &redisInstance)

		instanceArn, err := getElasticacheInstanceArn(elasticacheClient, redisInstance)
		if err != nil {
			return fmt.Errorf("could not get ARN for cluster %s: %s", redisInstance.ClusterID, err)
		}
		if instanceArn == "" {
			continue
		}

		plan, _ := catalog.RedisService.FetchPlan(redisInstance.PlanID)
		if plan.Name == "" {
			return fmt.Errorf("error getting plan %s for cluster %s", redisInstance.PlanID, redisInstance.ClusterID)
		}

		generatedTags, err := tags.GenerateTags(
			tagManager,
			catalog.RedisService.Name,
			plan.Name,
			brokertags.ResourceGUIDs{
				InstanceGUID:     redisInstance.Uuid,
				SpaceGUID:        redisInstance.SpaceGUID,
				OrganizationGUID: redisInstance.OrganizationGUID,
			},
		)
		if err != nil {
			return fmt.Errorf("error generating new tags for cluster %s: %s", redisInstance.ClusterID, err)
		}

		generatedElasticacheTags := redis.ConvertTagsToElasticacheTags(generatedTags)

		err = processElasticacheResource(elasticacheClient, instanceArn, generatedElasticacheTags)
		if err != nil {
			return err
		}
	}

	return nil
}
