package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
)

func TestDoExistingTagsMatchNewTags(t *testing.T) {
	existingTags := []*awsRds.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}
	newTags := []*awsRds.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}
	if !doExistingTagsMatchNewTags(existingTags, newTags) {
		t.Error("expected doExistingTagsMatchNewTags to return true")
	}
}
