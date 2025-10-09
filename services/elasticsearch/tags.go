package elasticsearch

import (
	"github.com/aws/aws-sdk-go-v2/aws"

	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
)

func ConvertTagsToOpensearchTags(tags map[string]string) []opensearchTypes.Tag {
	var opensearchTags []opensearchTypes.Tag
	for k, v := range tags {
		tag := opensearchTypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		opensearchTags = append(opensearchTags, tag)
	}
	return opensearchTags
}
