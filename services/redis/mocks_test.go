package redis

import (
	"context"
	"os"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/config"
)

func NewTestDedicatedDBAdapter(s *config.Settings, elasticache ElasticacheClientInterface, s3 brokerAws.S3ClientInterface) *dedicatedRedisAdapter {
	logger := lager.NewLogger("aws-redis-test")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.INFO))
	return NewRedisDedicatedDBAdapter(s, elasticache, s3, logger)
}

type mockRedisClient struct {
	modifyReplicationGroupErr error
}

func (m *mockRedisClient) CopySnapshot(ctx context.Context, params *elasticache.CopySnapshotInput, optFns ...func(*elasticache.Options)) (*elasticache.CopySnapshotOutput, error) {
	return nil, nil
}

func (m *mockRedisClient) CreateReplicationGroup(ctx context.Context, params *elasticache.CreateReplicationGroupInput, optFns ...func(*elasticache.Options)) (*elasticache.CreateReplicationGroupOutput, error) {
	return nil, nil
}

func (m *mockRedisClient) DeleteReplicationGroup(ctx context.Context, params *elasticache.DeleteReplicationGroupInput, optFns ...func(*elasticache.Options)) (*elasticache.DeleteReplicationGroupOutput, error) {
	return nil, nil
}

func (m *mockRedisClient) DeleteSnapshot(ctx context.Context, params *elasticache.DeleteSnapshotInput, optFns ...func(*elasticache.Options)) (*elasticache.DeleteSnapshotOutput, error) {
	return nil, nil
}

func (m *mockRedisClient) DescribeReplicationGroups(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error) {
	return nil, nil
}

func (m *mockRedisClient) DescribeSnapshots(ctx context.Context, params *elasticache.DescribeSnapshotsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeSnapshotsOutput, error) {
	return nil, nil
}

func (m *mockRedisClient) ModifyReplicationGroup(ctx context.Context, params *elasticache.ModifyReplicationGroupInput, optFns ...func(*elasticache.Options)) (*elasticache.ModifyReplicationGroupOutput, error) {
	return nil, m.modifyReplicationGroupErr
}

type mockS3Client struct{}

func (s *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, nil
}
