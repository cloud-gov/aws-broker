package elasticsearch

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/opensearchservice"
)

func ConvertTagsToOpensearchTags(tags map[string]string) []*opensearchservice.Tag {
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
