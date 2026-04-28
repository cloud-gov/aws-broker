package elasticsearch

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"

	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type STSClientInterface interface {
	GetCallerIdentity(ctx context.Context, params *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

type OpensearchClientInterface interface {
	CreateDomain(ctx context.Context, params *opensearch.CreateDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.CreateDomainOutput, error)
	DeleteDomain(ctx context.Context, params *opensearch.DeleteDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DeleteDomainOutput, error)
	DescribeDomain(ctx context.Context, params *opensearch.DescribeDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DescribeDomainOutput, error)
	UpdateDomainConfig(ctx context.Context, params *opensearch.UpdateDomainConfigInput, optFns ...func(*opensearch.Options)) (*opensearch.UpdateDomainConfigOutput, error)
}

var opensearchVolumeTypeMap = map[string]opensearchTypes.VolumeType{
	"gp3": opensearchTypes.VolumeTypeGp3,
}

func getOpensearchVolumeTypeEnum(volumeTypeString string) (*opensearchTypes.VolumeType, error) {
	if volumeType, ok := opensearchVolumeTypeMap[volumeTypeString]; ok {
		return &volumeType, nil
	}
	return nil, fmt.Errorf("invalid volume type: %s", volumeTypeString)
}

var opensearchInstanceTypeMap = map[string]opensearchTypes.OpenSearchPartitionInstanceType{
	"t3.small.search":    opensearchTypes.OpenSearchPartitionInstanceTypeT3SmallSearch,
	"c5.large.search":    opensearchTypes.OpenSearchPartitionInstanceTypeC5LargeSearch,
	"c5.xlarge.search":   opensearchTypes.OpenSearchPartitionInstanceTypeC5XlargeSearch,
	"c5.2xlarge.search":  opensearchTypes.OpenSearchPartitionInstanceTypeC52xlargeSearch,
	"m5.2xlarge.search":  opensearchTypes.OpenSearchPartitionInstanceTypeM52xlargeSearch,
	"m5.4xlarge.search":  opensearchTypes.OpenSearchPartitionInstanceTypeM54xlargeSearch,
	"m5.12xlarge.search": opensearchTypes.OpenSearchPartitionInstanceTypeM512xlargeSearch,
}

func getOpensearchInstanceTypeEnum(instanceTypeString string) (*opensearchTypes.OpenSearchPartitionInstanceType, error) {
	if instanceType, ok := opensearchInstanceTypeMap[instanceTypeString]; ok {
		return &instanceType, nil
	}
	return nil, fmt.Errorf("invalid instance type: %s", instanceTypeString)
}
