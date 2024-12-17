package rds

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
)

func convertTagsToRDSTags(tags map[string]string) []*rds.Tag {
	var rdsTags []*rds.Tag
	for k, v := range tags {
		tag := rds.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		rdsTags = append(rdsTags, &tag)
	}
	return rdsTags
}
