package rds

import (
	"github.com/aws/aws-sdk-go-v2/aws"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func ConvertTagsToRDSTags(tags map[string]string) []rdsTypes.Tag {
	var rdsTags []rdsTypes.Tag
	for k, v := range tags {
		tag := rdsTypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		rdsTags = append(rdsTags, tag)
	}
	return rdsTags
}
