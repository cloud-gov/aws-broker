package rds

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/services/rds"
	brokertags "github.com/cloud-gov/go-broker-tags"
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

func (m mockRdsClient) AddTagsToResource(*awsRds.AddTagsToResourceInput) (*awsRds.AddTagsToResourceOutput, error) {
	return nil, nil
}

func (m mockRdsClient) DescribeDBParameterGroups(*awsRds.DescribeDBParameterGroupsInput) (*awsRds.DescribeDBParameterGroupsOutput, error) {
	return nil, nil
}

type mockTagManager struct {
	brokertags.TagManager

	generatedTags map[string]string
}

func (t mockTagManager) GenerateTags(
	action brokertags.Action,
	serviceName string,
	servicePlanName string,
	resourceGUIDs brokertags.ResourceGUIDs,
	getMissingResources bool,
) (map[string]string, error) {
	return t.generatedTags, nil
}

type mockLogsClient struct {
	cloudwatchlogsiface.CloudWatchLogsAPI

	logGroups            []*cloudwatchlogs.LogGroup
	describeLogGroupsErr error
	tagResourceErr       error
}

func (l mockLogsClient) DescribeLogGroups(*cloudwatchlogs.DescribeLogGroupsInput) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	if l.describeLogGroupsErr != nil {
		return nil, l.describeLogGroupsErr
	}
	return &cloudwatchlogs.DescribeLogGroupsOutput{
		LogGroups: l.logGroups,
	}, nil
}

func (l mockLogsClient) TagResource(*cloudwatchlogs.TagResourceInput) (*cloudwatchlogs.TagResourceOutput, error) {
	return nil, l.tagResourceErr
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
	tags, err := getRDSResourceTags(mockClient, "fake-arn")
	if err != nil {
		t.Error(err)
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
			doTagsMatch := doRDSResourceTagsContainGeneratedTags(test.existingRdsTags, test.generatedRdsTags)
			if doTagsMatch != test.shouldTagsMatch {
				t.Errorf("expected doRDSTagsContainGeneratedTags to return %t, got: %t", test.shouldTagsMatch, doTagsMatch)
			}
		})
	}
}

func TestReconcileRDSResourceTagsSuccess(t *testing.T) {
	testCases := map[string]struct {
		rdsInstance    rds.RDSInstance
		catalog        *catalog.Catalog
		mockLogsClient *mockLogsClient
		mockRdsClient  *mockRdsClient
		mockTagManager *mockTagManager
		expectErr      bool
	}{
		"success": {
			rdsInstance: rds.RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						PlanID: "plan-1",
					},
				},
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
								ID:   "plan-1",
								Name: "plan-1",
							},
						},
					},
				},
			},
			mockLogsClient: &mockLogsClient{},
			mockRdsClient:  &mockRdsClient{},
			mockTagManager: &mockTagManager{},
		},
		"error fetching plan": {
			rdsInstance: rds.RDSInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						PlanID: "plan-1",
					},
				},
			},
			catalog: &catalog.Catalog{
				RdsService: catalog.RDSService{
					Plans: []catalog.RDSPlan{
						{
							Plan: catalog.Plan{
								ID:   "plan-2",
								Name: "plan-2",
							},
						},
					},
				},
			},
			mockLogsClient: &mockLogsClient{},
			mockRdsClient:  &mockRdsClient{},
			mockTagManager: &mockTagManager{},
			expectErr:      true,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := reconcileResourceTagsForRDSDatabase(
				test.rdsInstance,
				test.catalog,
				test.mockRdsClient,
				test.mockLogsClient,
				test.mockTagManager,
			)
			if err != nil && !test.expectErr {
				t.Error(err)
			}
			if err == nil && test.expectErr {
				t.Error("expected error, but received nil")
			}
		})
	}
}
