package opensearch

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/cmd/tasks/tags"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
)

func getOpensearchDomainArn(opensearchClient OpensearchClientInterface, elasticsearchInstance elasticsearch.ElasticsearchInstance) (string, error) {
	instanceInfo, err := opensearchClient.DescribeDomain(context.TODO(), &opensearch.DescribeDomainInput{
		DomainName: aws.String(elasticsearchInstance.Domain),
	})

	if err != nil {
		var notFoundException *opensearchTypes.ResourceNotFoundException
		if errors.As(err, &notFoundException) {
			log.Printf("Could not find domain %s, continuing", elasticsearchInstance.Domain)
			return "", nil
		} else {
			return "", fmt.Errorf("could not describe domain: %s", err)
		}
	}

	return *instanceInfo.DomainStatus.ARN, nil
}

func getOpensearchResourceTags(opensearchClient OpensearchClientInterface, instanceArn string) ([]opensearchTypes.Tag, error) {
	response, err := opensearchClient.ListTags(context.TODO(), &opensearch.ListTagsInput{
		ARN: aws.String(instanceArn),
	})
	if err != nil {
		return []opensearchTypes.Tag{}, fmt.Errorf("error getting tags for domain %s: %s", instanceArn, err)
	}
	return response.TagList, nil
}

func doOpensearchResourceTagsContainGeneratedTags(rdsTags []opensearchTypes.Tag, generatedTags []opensearchTypes.Tag) bool {
	for _, v := range generatedTags {
		if slices.Contains([]string{"Created at", "Updated at"}, *v.Key) {
			continue
		}

		if !slices.ContainsFunc(rdsTags, func(tag opensearchTypes.Tag) bool {
			return *tag.Key == *v.Key && *tag.Value == *v.Value
		}) {
			return false
		}
	}
	return true
}

func processOpensearchResource(opensearchClient OpensearchClientInterface, resourceArn string, generatedTags []opensearchTypes.Tag) error {
	existingInstanceTags, err := getOpensearchResourceTags(opensearchClient, resourceArn)
	if err != nil {
		return fmt.Errorf("could not get tags for resource %s: %s", resourceArn, err)
	}

	if doOpensearchResourceTagsContainGeneratedTags(existingInstanceTags, generatedTags) {
		log.Printf("tags already updated for resource %s", resourceArn)
		return nil
	}

	log.Printf("updating tags for resource %s", resourceArn)
	_, err = opensearchClient.AddTags(context.TODO(), &opensearch.AddTagsInput{
		ARN:     aws.String(resourceArn),
		TagList: generatedTags,
	})
	if err != nil {
		return fmt.Errorf("error adding new tags for resource %s: %s", resourceArn, err)
	}

	log.Printf("finished updating tags for resource %s", resourceArn)
	return nil
}

func ReconcileOpensearchResourceTags(catalog *catalog.Catalog, db *gorm.DB, opensearchClient OpensearchClientInterface, tagManager brokertags.TagManager) error {
	rows, err := db.Model(&elasticsearch.ElasticsearchInstance{}).Rows()
	if err != nil {
		return err
	}

	for rows.Next() {
		var elasticsearchInstance elasticsearch.ElasticsearchInstance
		db.ScanRows(rows, &elasticsearchInstance)

		instanceArn, err := getOpensearchDomainArn(opensearchClient, elasticsearchInstance)
		if err != nil {
			return fmt.Errorf("could not get ARN for domain %s: %s", elasticsearchInstance.Domain, err)
		}
		if instanceArn == "" {
			continue
		}

		plan, _ := catalog.ElasticsearchService.FetchPlan(elasticsearchInstance.PlanID)
		if plan.Name == "" {
			return fmt.Errorf("error getting plan %s for domain %s", elasticsearchInstance.PlanID, elasticsearchInstance.Domain)
		}

		generatedTags, err := tags.GenerateTags(
			tagManager,
			catalog.ElasticsearchService.Name,
			plan.Name,
			brokertags.ResourceGUIDs{
				InstanceGUID:     elasticsearchInstance.Uuid,
				SpaceGUID:        elasticsearchInstance.SpaceGUID,
				OrganizationGUID: elasticsearchInstance.OrganizationGUID,
			},
		)
		if err != nil {
			return fmt.Errorf("error generating new tags for domain %s: %s", elasticsearchInstance.Domain, err)
		}

		generatedOpensearchTags := elasticsearch.ConvertTagsToOpensearchTags(generatedTags)
		err = processOpensearchResource(opensearchClient, instanceArn, generatedOpensearchTags)
		if err != nil {
			return err
		}
	}

	return nil
}
