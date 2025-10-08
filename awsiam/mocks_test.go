package awsiam

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
)

var mockPolDoc string = `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": ["some:action"],"Resource": ["some:resource"]}]}`

type MockIAMClient struct {
	attachedUserPolicies []types.AttachedPolicy
	attachedRolePolicies []types.AttachedPolicy

	createRoleErr      error
	createPolicyErr    error
	createPolicyInputs []*iam.CreatePolicyInput

	getUserInputName string
	getUserOutput    *iam.GetUserOutput

	listPolicyVersionsOutput iam.ListPolicyVersionsOutput
	listPolicyVersionsErr    error

	deletePolicyOutput iam.DeletePolicyOutput
	deletePolicyErr    error

	deletePolicyVersionOutput  iam.DeletePolicyVersionOutput
	deletePolicyVersionErr     error
	deletedPolicyVersionInputs []*iam.DeletePolicyVersionInput
}

func (i *MockIAMClient) AttachRolePolicy(ctx context.Context, params *iam.AttachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	return &iam.AttachRolePolicyOutput{}, nil
}

func (i *MockIAMClient) AttachUserPolicy(ctx context.Context, params *iam.AttachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.AttachUserPolicyOutput, error) {
	return &iam.AttachUserPolicyOutput{}, nil
}

func (i *MockIAMClient) CreateAccessKey(ctx context.Context, params *iam.CreateAccessKeyInput, optFns ...func(*iam.Options)) (*iam.CreateAccessKeyOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) CreatePolicy(ctx context.Context, params *iam.CreatePolicyInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
	if i.createPolicyErr != nil {
		return nil, i.createPolicyErr
	}
	i.createPolicyInputs = append(i.createPolicyInputs, params)
	arn := "arn:aws:iam::123456789012:policy/" + *(params.PolicyName)
	return &iam.CreatePolicyOutput{
		Policy: &types.Policy{
			Arn:        aws.String(arn),
			PolicyName: params.PolicyName,
			Tags:       params.Tags,
		},
	}, nil
}

func (i *MockIAMClient) CreatePolicyVersion(ctx context.Context, params *iam.CreatePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	return &iam.CreatePolicyVersionOutput{
		PolicyVersion: &types.PolicyVersion{
			VersionId:        aws.String("new"),
			Document:         params.PolicyDocument,
			IsDefaultVersion: params.SetAsDefault,
		},
	}, nil
}

func (i *MockIAMClient) CreateRole(ctx context.Context, params *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	if i.createRoleErr != nil {
		return nil, i.createRoleErr
	}
	arn := "arn:aws:iam::123456789012:role/" + *(params.RoleName)
	return &iam.CreateRoleOutput{
		Role: &types.Role{
			Arn:                      aws.String(arn),
			RoleName:                 params.RoleName,
			AssumeRolePolicyDocument: params.AssumeRolePolicyDocument,
			Tags:                     params.Tags,
		},
	}, nil
}

func (i *MockIAMClient) CreateUser(ctx context.Context, params *iam.CreateUserInput, optFns ...func(*iam.Options)) (*iam.CreateUserOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) DeleteAccessKey(ctx context.Context, params *iam.DeleteAccessKeyInput, optFns ...func(*iam.Options)) (*iam.DeleteAccessKeyOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) DeletePolicy(ctx context.Context, params *iam.DeletePolicyInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	return &i.deletePolicyOutput, i.deletePolicyErr
}

func (i *MockIAMClient) DeleteRole(ctx context.Context, params *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) DeletePolicyVersion(ctx context.Context, params *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	if i.deletePolicyVersionErr != nil {
		return nil, i.deletePolicyVersionErr
	}
	i.deletedPolicyVersionInputs = append(i.deletedPolicyVersionInputs, params)
	return &i.deletePolicyVersionOutput, nil
}

func (i *MockIAMClient) DeleteUser(ctx context.Context, params *iam.DeleteUserInput, optFns ...func(*iam.Options)) (*iam.DeleteUserOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) DetachRolePolicy(ctx context.Context, params *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) DetachUserPolicy(ctx context.Context, params *iam.DetachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) GetPolicy(ctx context.Context, params *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	return &iam.GetPolicyOutput{
		Policy: &types.Policy{
			Arn:              params.PolicyArn,
			DefaultVersionId: aws.String("old"),
		},
	}, nil
}

func (i *MockIAMClient) GetPolicyVersion(ctx context.Context, params *iam.GetPolicyVersionInput, optFns ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	return &iam.GetPolicyVersionOutput{
		PolicyVersion: &types.PolicyVersion{
			Document:         aws.String(mockPolDoc),
			IsDefaultVersion: true,
			VersionId:        params.VersionId,
		},
	}, nil
}

func (i *MockIAMClient) GetRole(ctx context.Context, params *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	arn := "arn:aws:iam::123456789012:role/" + *(params.RoleName)
	return &iam.GetRoleOutput{
		Role: &types.Role{
			Arn:      aws.String(arn),
			RoleName: params.RoleName,
		},
	}, nil
}

func (i *MockIAMClient) GetUser(ctx context.Context, params *iam.GetUserInput, optFns ...func(*iam.Options)) (*iam.GetUserOutput, error) {
	if i.getUserInputName != "" && i.getUserInputName == *params.UserName {
		return i.getUserOutput, nil
	}
	return nil, nil
}

func (i *MockIAMClient) ListAccessKeys(ctx context.Context, params *iam.ListAccessKeysInput, optFns ...func(*iam.Options)) (*iam.ListAccessKeysOutput, error) {
	return nil, nil
}

func (i *MockIAMClient) ListAttachedRolePolicies(ctx context.Context, params *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	return &iam.ListAttachedRolePoliciesOutput{
		AttachedPolicies: i.attachedRolePolicies,
	}, nil
}

func (i *MockIAMClient) ListAttachedUserPolicies(ctx context.Context, params *iam.ListAttachedUserPoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedUserPoliciesOutput, error) {
	return &iam.ListAttachedUserPoliciesOutput{
		AttachedPolicies: i.attachedUserPolicies,
	}, nil
}

func (i *MockIAMClient) ListPolicyVersions(ctx context.Context, params *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	return &i.listPolicyVersionsOutput, i.listPolicyVersionsErr
}
