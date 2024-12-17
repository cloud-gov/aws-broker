package main

import (
	"fmt"
	"log"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/services/elasticsearch"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/opensearchservice"
	"github.com/aws/aws-sdk-go/service/opensearchservice/opensearchserviceiface"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/jinzhu/gorm"
	"golang.org/x/exp/slices"
)

func getOpensearchInstanceArn(opensearchClient opensearchserviceiface.OpenSearchServiceAPI, elasticsearchInstance elasticsearch.ElasticsearchInstance) (string, error) {
	instanceInfo, err := opensearchClient.DescribeDomain(&opensearchservice.DescribeDomainInput{
		DomainName: aws.String(elasticsearchInstance.Domain),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == opensearchservice.ErrCodeResourceNotFoundException {
				log.Printf("Could not find domain %s, continuing", elasticsearchInstance.Domain)
				return "", nil
			} else {
				return "", fmt.Errorf("could not describe domain: %s", err)
			}
		} else {
			return "", fmt.Errorf("could not describe domain: %s", err)
		}
	}

	return *instanceInfo.DomainStatus.ARN, nil
}

func getOpensearchInstanceTags(opensearchClient opensearchserviceiface.OpenSearchServiceAPI, instanceArn string) ([]*opensearchservice.Tag, error) {
	response, err := opensearchClient.ListTags(&opensearchservice.ListTagsInput{
		ARN: aws.String(instanceArn),
	})
	if err != nil {
		return []*opensearchservice.Tag{}, fmt.Errorf("error getting tags for domain %s: %s", instanceArn, err)
	}
	return response.TagList, nil
}

func convertTagsToOpensearchTags(tags map[string]string) []*opensearchservice.Tag {
	var opensearchTags []*opensearchservice.Tag
	for k, v := range tags {
		tag := opensearchservice.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		opensearchTags = append(opensearchTags, &tag)
	}
	return opensearchTags
}

func doOpensearchInstanceTagsContainGeneratedTags(rdsTags []*opensearchservice.Tag, generatedTags []*opensearchservice.Tag) bool {
	for _, v := range generatedTags {
		if slices.Contains([]string{"Created at", "Updated at"}, *v.Key) {
			continue
		}

		if !slices.ContainsFunc(rdsTags, func(tag *opensearchservice.Tag) bool {
			return *tag.Key == *v.Key && *tag.Value == *v.Value
		}) {
			return false
		}
	}
	return true
}

func fetchAndUpdateOpensearchInstanceTags(catalog *catalog.Catalog, db *gorm.DB, opensearchClient opensearchserviceiface.OpenSearchServiceAPI, tagManager brokertags.TagManager) {
	rows, err := db.Model(&elasticsearch.ElasticsearchInstance{}).Rows()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var elasticsearchInstance elasticsearch.ElasticsearchInstance
		db.ScanRows(rows, &elasticsearchInstance)

		instanceArn, err := getOpensearchInstanceArn(opensearchClient, elasticsearchInstance)
		if err != nil {
			log.Fatalf("could not get ARN for domain %s: %s", elasticsearchInstance.Domain, err)
		}
		if instanceArn == "" {
			continue
		}

		existingInstanceTags, err := getOpensearchInstanceTags(opensearchClient, instanceArn)
		if err != nil {
			log.Fatalf("could not get tags for domain %s: %s", elasticsearchInstance.Domain, err)
		}

		plan, _ := catalog.ElasticsearchService.FetchPlan(elasticsearchInstance.PlanID)
		if plan.Name == "" {
			log.Fatalf("error getting plan %s for domain %s", elasticsearchInstance.PlanID, elasticsearchInstance.Domain)
		}

		generatedTags, err := generateTags(
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
			log.Fatalf("error generating new tags for domain %s: %s", elasticsearchInstance.Domain, err)
		}

		generatedOpensearchTags := convertTagsToOpensearchTags(generatedTags)
		if doOpensearchInstanceTagsContainGeneratedTags(existingInstanceTags, generatedOpensearchTags) {
			log.Printf("tags already updated for domain %s", elasticsearchInstance.Domain)
			continue
		}

		log.Printf("updating tags for domain %s", elasticsearchInstance.Domain)
		_, err = opensearchClient.AddTags(&opensearchservice.AddTagsInput{
			ARN:     aws.String(instanceArn),
			TagList: generatedOpensearchTags,
		})
		if err != nil {
			log.Fatalf("error adding new tags for domain %s: %s", elasticsearchInstance.Domain, err)
		}

		log.Printf("finished updating tags for domain %s", elasticsearchInstance.Domain)
	}
}
