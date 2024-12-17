package main

import (
	"testing"
	"time"

	"github.com/18F/aws-broker/services/rds"
	"github.com/aws/aws-sdk-go/aws"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/go-test/deep"
)

type mockRdsClient struct {
	rdsiface.RDSAPI

	tags []*awsRds.Tag
}

func (m mockRdsClient) DescribeDBInstances(*awsRds.DescribeDBInstancesInput) (*awsRds.DescribeDBInstancesOutput, error) {
	return &awsRds.DescribeDBInstancesOutput{
		DBInstances: []*awsRds.DBInstance{
			{
				DBInstanceArn: aws.String("fake-db-arn"),
			},
		},
	}, nil
}

func (m mockRdsClient) ListTagsForResource(*awsRds.ListTagsForResourceInput) (*awsRds.ListTagsForResourceOutput, error) {
	return &awsRds.ListTagsForResourceOutput{
		TagList: m.tags,
	}, nil
}

func TestGetRdsInstanceTags(t *testing.T) {
	mockClient := mockRdsClient{
		tags: []*awsRds.Tag{
			{
				Key:   aws.String("foo"),
				Value: aws.String("bar"),
			},
		},
	}
	instance := &rds.RDSInstance{
		Database: "fake-database",
	}
	tags, shouldContinue, err := getRdsInstanceTags(mockClient, instance)
	if err != nil {
		t.Error(err)
	}
	if !shouldContinue {
		t.Error("expected shouldContinue to be true")
	}
	expectedTags := []*awsRds.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}
	if diff := deep.Equal(tags, expectedTags); diff != nil {
		t.Error(diff)
	}
}

func TestDoExistingTagsMatchNewTags(t *testing.T) {
	testCases := map[string]struct {
		existingRdsTags  []*awsRds.Tag
		generatedRdsTags []*awsRds.Tag
		shouldTagsMatch  bool
	}{
		"different key order": {
			existingRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
				{
					Key:   aws.String("moo"),
					Value: aws.String("cow"),
				},
			},
			generatedRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("moo"),
					Value: aws.String("cow"),
				},
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
			},
			shouldTagsMatch: true,
		},
		"different Created at times": {
			existingRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
				{
					Key:   aws.String("Created at"),
					Value: aws.String(time.Now().String()),
				},
			},
			generatedRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
				{
					Key:   aws.String("Created at"),
					Value: aws.String(time.Now().String()),
				},
			},
			shouldTagsMatch: true,
		},
		"different Updated at times": {
			existingRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
				{
					Key:   aws.String("Updated at"),
					Value: aws.String(time.Now().String()),
				},
			},
			generatedRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
				{
					Key:   aws.String("Updated at"),
					Value: aws.String(time.Now().String()),
				},
			},
			shouldTagsMatch: true,
		},
		"should not match": {
			existingRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
			},
			generatedRdsTags: []*awsRds.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("cow"),
				},
			},
			shouldTagsMatch: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			doTagsMatch := doRDSTagsContainGeneratedTags(test.existingRdsTags, test.generatedRdsTags)
			if doTagsMatch != test.shouldTagsMatch {
				t.Errorf("expected doRDSTagsContainGeneratedTags to return %t, got: %t", test.shouldTagsMatch, doTagsMatch)
			}
		})
	}
}
