package elasticsearch

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
)

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	// Automigrate!
	err = db.AutoMigrate(&ElasticsearchInstance{}, &base.Instance{}, &asyncmessage.AsyncJobMsg{})
	return db, err
}

type mockOpensearchClient struct {
	describeDomainCallNum int
	describeDomainErrs    []error
	describeDomainResults []*opensearch.DescribeDomainOutput

	upgradeDomainInput *opensearch.UpgradeDomainInput
	upgradeDomainErr   error

	updateDomainConfigErr error

	compatibleVersions    []opensearchTypes.CompatibleVersionsMap
	compatibleVersionsErr error
}

func (o *mockOpensearchClient) CreateDomain(ctx context.Context, params *opensearch.CreateDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.CreateDomainOutput, error) {
	return nil, nil
}

func (o *mockOpensearchClient) DeleteDomain(ctx context.Context, params *opensearch.DeleteDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DeleteDomainOutput, error) {
	return nil, nil
}

func (o *mockOpensearchClient) DescribeDomain(ctx context.Context, params *opensearch.DescribeDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DescribeDomainOutput, error) {
	if len(o.describeDomainErrs) > 0 && o.describeDomainErrs[o.describeDomainCallNum] != nil {
		return nil, o.describeDomainErrs[o.describeDomainCallNum]
	}
	if len(o.describeDomainResults) > 0 {
		result := o.describeDomainResults[o.describeDomainCallNum]
		o.describeDomainCallNum++
		return result, nil
	}
	return nil, nil
}

func (o *mockOpensearchClient) UpdateDomainConfig(ctx context.Context, params *opensearch.UpdateDomainConfigInput, optFns ...func(*opensearch.Options)) (*opensearch.UpdateDomainConfigOutput, error) {
	return nil, o.updateDomainConfigErr
}

func (o *mockOpensearchClient) UpgradeDomain(ctx context.Context, params *opensearch.UpgradeDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.UpgradeDomainOutput, error) {
	o.upgradeDomainInput = params
	return &opensearch.UpgradeDomainOutput{}, o.upgradeDomainErr
}

func (o *mockOpensearchClient) GetCompatibleVersions(ctx context.Context, params *opensearch.GetCompatibleVersionsInput, optFns ...func(*opensearch.Options)) (*opensearch.GetCompatibleVersionsOutput, error) {
	return &opensearch.GetCompatibleVersionsOutput{CompatibleVersions: o.compatibleVersions}, o.compatibleVersionsErr
}
