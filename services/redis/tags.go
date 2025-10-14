package redis

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	elasticacheTypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
)

func ConvertTagsToElasticacheTags(tags map[string]string) []elasticacheTypes.Tag {
	var elasticacheTags []elasticacheTypes.Tag
	for k, v := range tags {
		tag := elasticacheTypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		elasticacheTags = append(elasticacheTags, tag)
	}
	return elasticacheTags
}
