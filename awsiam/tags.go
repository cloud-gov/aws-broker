package awsiam

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
)

func ConvertTagsMapToIAMTags(
	tags map[string]string,
) []*iam.Tag {
	var awsTags []*iam.Tag
	for k, v := range tags {
		awsTags = append(awsTags, &iam.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return awsTags
}
