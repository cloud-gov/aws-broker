package elasticsearch

import (
	"context"

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
