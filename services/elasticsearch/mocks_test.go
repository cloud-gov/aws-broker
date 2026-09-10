package elasticsearch

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
)

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	if err != nil {
		return nil, err
	}
	// Automigrate!
	err = db.AutoMigrate(&ElasticsearchInstance{}, &base.Instance{}, &asyncmessage.AsyncJobMsg{})
	return db, err
}

type mockOpensearchClient struct {
	createDomainOutput *opensearch.CreateDomainOutput

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
	return o.createDomainOutput, nil
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

type mockCloudwatchLogsClient struct {
	createdLogGroups      []string
	deletedLogGroups      []string
	putRetentionLogGroups []string
	createLogGroupErr     error
	putRetentionPolicyErr error
	deleteLogGroupErr     error
}

func (m *mockCloudwatchLogsClient) CreateLogGroup(ctx context.Context, params *cloudwatchlogs.CreateLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error) {
	if m.createLogGroupErr != nil {
		return nil, m.createLogGroupErr
	}
	m.createdLogGroups = append(m.createdLogGroups, *params.LogGroupName)
	return &cloudwatchlogs.CreateLogGroupOutput{}, nil
}

func (m *mockCloudwatchLogsClient) PutRetentionPolicy(ctx context.Context, params *cloudwatchlogs.PutRetentionPolicyInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutRetentionPolicyOutput, error) {
	if m.putRetentionPolicyErr != nil {
		return nil, m.putRetentionPolicyErr
	}
	m.putRetentionLogGroups = append(m.putRetentionLogGroups, *params.LogGroupName)
	return &cloudwatchlogs.PutRetentionPolicyOutput{}, nil
}

func (m *mockCloudwatchLogsClient) DescribeLogGroups(ctx context.Context, params *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	return &cloudwatchlogs.DescribeLogGroupsOutput{}, nil
}

func (m *mockCloudwatchLogsClient) DeleteLogGroup(ctx context.Context, params *cloudwatchlogs.DeleteLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DeleteLogGroupOutput, error) {
	if m.deleteLogGroupErr != nil {
		return nil, m.deleteLogGroupErr
	}
	m.deletedLogGroups = append(m.deletedLogGroups, *params.LogGroupName)
	return &cloudwatchlogs.DeleteLogGroupOutput{}, nil
}

type mockEsApiClient struct {
	getSnapshotStatusCallNum   int
	getSnapshotStatusResponses []string
	getSnapshotStatusErrs      []error
}

func (m *mockEsApiClient) CreateSnapshotRepo(repositoryName string, bucketName string, path string, region string, roleArn string) (string, error) {
	return "", nil
}

func (m *mockEsApiClient) CreateSnapshot(repositoryName string, snapshotName string) (string, error) {
	return "", nil
}

func (m *mockEsApiClient) GetSnapshotStatus(repositoryName string, snapshotName string) (string, error) {
	currentCallNum := m.getSnapshotStatusCallNum
	m.getSnapshotStatusCallNum++
	if len(m.getSnapshotStatusErrs) > 0 && m.getSnapshotStatusErrs[currentCallNum] != nil {
		return "", m.getSnapshotStatusErrs[currentCallNum]
	}
	status := m.getSnapshotStatusResponses[currentCallNum]
	return status, nil
}

func (m *mockEsApiClient) EnableAuditLogging(engineVersion string) error {
	return nil
}

type mockS3Client struct {
	putObjectErr error
}

func (s *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, s.putObjectErr
}

type mockIamClient struct {
	createAccessKeyOutput    *iam.CreateAccessKeyOutput
	createPolicyOutput       *iam.CreatePolicyOutput
	createRoleCallNum        int
	createRoleOutput         []*iam.CreateRoleOutput
	getUserOutput            *iam.GetUserOutput
	listPolicyVersionsOutput *iam.ListPolicyVersionsOutput
}

func (m *mockIamClient) CreateAccessKey(ctx context.Context, params *iam.CreateAccessKeyInput, optFns ...func(*iam.Options)) (*iam.CreateAccessKeyOutput, error) {
	return m.createAccessKeyOutput, nil
}

func (m *mockIamClient) CreatePolicy(ctx context.Context, params *iam.CreatePolicyInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
	return m.createPolicyOutput, nil
}

func (m *mockIamClient) DeleteAccessKey(ctx context.Context, params *iam.DeleteAccessKeyInput, optFns ...func(*iam.Options)) (*iam.DeleteAccessKeyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeleteRole(ctx context.Context, params *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeleteUser(ctx context.Context, params *iam.DeleteUserInput, optFns ...func(*iam.Options)) (*iam.DeleteUserOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DetachRolePolicy(ctx context.Context, params *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DetachUserPolicy(ctx context.Context, params *iam.DetachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) AttachRolePolicy(ctx context.Context, params *iam.AttachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) AttachUserPolicy(ctx context.Context, params *iam.AttachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.AttachUserPolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) CreatePolicyVersion(ctx context.Context, params *iam.CreatePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	return nil, nil
}

func (m *mockIamClient) CreateRole(ctx context.Context, params *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	output := m.createRoleOutput[m.createRoleCallNum]
	m.createRoleCallNum++
	return output, nil
}

func (m *mockIamClient) CreateUser(ctx context.Context, params *iam.CreateUserInput, optFns ...func(*iam.Options)) (*iam.CreateUserOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeletePolicy(ctx context.Context, params *iam.DeletePolicyInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) DeletePolicyVersion(ctx context.Context, params *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetPolicy(ctx context.Context, params *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetPolicyVersion(ctx context.Context, params *iam.GetPolicyVersionInput, optFns ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetRole(ctx context.Context, params *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	return nil, nil
}

func (m *mockIamClient) GetUser(ctx context.Context, params *iam.GetUserInput, optFns ...func(*iam.Options)) (*iam.GetUserOutput, error) {
	return m.getUserOutput, nil
}

func (m *mockIamClient) ListAttachedRolePolicies(ctx context.Context, params *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	return nil, nil
}

func (m *mockIamClient) ListAttachedUserPolicies(ctx context.Context, params *iam.ListAttachedUserPoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedUserPoliciesOutput, error) {
	return nil, nil
}

func (m *mockIamClient) ListPolicyVersions(ctx context.Context, params *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	return m.listPolicyVersionsOutput, nil
}

type mockSTSClient struct {
	getCallerIdentityOutput *sts.GetCallerIdentityOutput
}

func (s *mockSTSClient) GetCallerIdentity(ctx context.Context, params *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return s.getCallerIdentityOutput, nil
}
