package redis

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/elasticache"
)

func ConvertTagsToElasticacheTags(tags map[string]string) []*elasticache.Tag {
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
