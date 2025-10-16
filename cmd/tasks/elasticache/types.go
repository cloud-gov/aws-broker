package elasticache

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

type ElasticacheClientInterface interface {
	AddTagsToResource(ctx context.Context, params *elasticache.AddTagsToResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.AddTagsToResourceOutput, error)
	DescribeReplicationGroups(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error)
	ListTagsForResource(ctx context.Context, params *elasticache.ListTagsForResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.ListTagsForResourceOutput, error)
}
