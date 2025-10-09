package awsiam

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

func ConvertTagsMapToIAMTags(
	tags map[string]string,
) []iamTypes.Tag {
	var awsTags []iamTypes.Tag
	for k, v := range tags {
		awsTags = append(awsTags, iamTypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return awsTags
}
