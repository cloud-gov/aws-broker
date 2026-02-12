package redis

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

type ElasticacheClientInterface interface {
	CopySnapshot(ctx context.Context, params *elasticache.CopySnapshotInput, optFns ...func(*elasticache.Options)) (*elasticache.CopySnapshotOutput, error)
	CreateReplicationGroup(ctx context.Context, params *elasticache.CreateReplicationGroupInput, optFns ...func(*elasticache.Options)) (*elasticache.CreateReplicationGroupOutput, error)
	DeleteReplicationGroup(ctx context.Context, params *elasticache.DeleteReplicationGroupInput, optFns ...func(*elasticache.Options)) (*elasticache.DeleteReplicationGroupOutput, error)
	DeleteSnapshot(ctx context.Context, params *elasticache.DeleteSnapshotInput, optFns ...func(*elasticache.Options)) (*elasticache.DeleteSnapshotOutput, error)
	DescribeReplicationGroups(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error)
	DescribeSnapshots(ctx context.Context, params *elasticache.DescribeSnapshotsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeSnapshotsOutput, error)
	ModifyReplicationGroup(ctx context.Context, params *elasticache.ModifyReplicationGroupInput, optFns ...func(*elasticache.Options)) (*elasticache.ModifyReplicationGroupOutput, error)
}
