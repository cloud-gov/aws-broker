package rds

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/rds"
)

type RDSClientInterface interface {
	AddTagsToResource(ctx context.Context, params *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error)
	DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
	DescribeDBParameterGroups(ctx context.Context, params *rds.DescribeDBParameterGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParameterGroupsOutput, error)
	ListTagsForResource(ctx context.Context, params *rds.ListTagsForResourceInput, optFns ...func(*rds.Options)) (*rds.ListTagsForResourceOutput, error)
}
