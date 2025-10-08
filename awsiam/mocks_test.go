package awsiam

import (
	"context"
	"errors"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
)

var logger lager.Logger = lager.NewLogger("awsiam-test")

var mockPolDoc string = `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": ["some:action"],"Resource": ["some:resource"]}]}`

type mockIAMClient struct {
	attachedRolePolicies []types.AttachedPolicy

	attachUserPolicyErr error

	createAccessKeyErr    error
	createAccessKeyOutput iam.CreateAccessKeyOutput

	createRoleErr      error
	createPolicyErr    error
	createPolicyInputs []*iam.CreatePolicyInput

	createUserErr    error
	createUserOutput *iam.CreateUserOutput

	getUserErr    error
	getUserOutput *iam.GetUserOutput

	iamPath string

	listAttachedUserPoliciesErr    error
	listAttachedUserPoliciesOutput iam.ListAttachedUserPoliciesOutput

	listPolicyVersionsOutput iam.ListPolicyVersionsOutput
	listPolicyVersionsErr    error

	deleteAccessKeyErr error

	deletePolicyOutput iam.DeletePolicyOutput
	deletePolicyErr    error

	deletePolicyVersionOutput  iam.DeletePolicyVersionOutput
	deletePolicyVersionErr     error
	deletedPolicyVersionInputs []*iam.DeletePolicyVersionInput

	deleteUserErr error

	detachUserPolicyErr error

	listAccessKeysErr    error
	listAccessKeysOutput iam.ListAccessKeysOutput

	policyArn string

	userName string
}

func (i *mockIAMClient) AttachRolePolicy(ctx context.Context, params *iam.AttachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	return &iam.AttachRolePolicyOutput{}, nil
}

func (i *mockIAMClient) AttachUserPolicy(ctx context.Context, params *iam.AttachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.AttachUserPolicyOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	if i.policyArn != "" && i.policyArn != *params.PolicyArn {
		return nil, errors.New("unexpected policy ARN")
	}
	return &iam.AttachUserPolicyOutput{}, i.attachUserPolicyErr
}

func (i *mockIAMClient) CreateAccessKey(ctx context.Context, params *iam.CreateAccessKeyInput, optFns ...func(*iam.Options)) (*iam.CreateAccessKeyOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	return &i.createAccessKeyOutput, i.createAccessKeyErr
}

func (i *mockIAMClient) CreatePolicy(ctx context.Context, params *iam.CreatePolicyInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
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

func (i *mockIAMClient) CreatePolicyVersion(ctx context.Context, params *iam.CreatePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	return &iam.CreatePolicyVersionOutput{
		PolicyVersion: &types.PolicyVersion{
			VersionId:        aws.String("new"),
			Document:         params.PolicyDocument,
			IsDefaultVersion: params.SetAsDefault,
		},
	}, nil
}

func (i *mockIAMClient) CreateRole(ctx context.Context, params *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
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

func (i *mockIAMClient) CreateUser(ctx context.Context, params *iam.CreateUserInput, optFns ...func(*iam.Options)) (*iam.CreateUserOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	return i.createUserOutput, i.createUserErr
}

func (i *mockIAMClient) DeleteAccessKey(ctx context.Context, params *iam.DeleteAccessKeyInput, optFns ...func(*iam.Options)) (*iam.DeleteAccessKeyOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	return nil, i.deleteAccessKeyErr
}

func (i *mockIAMClient) DeletePolicy(ctx context.Context, params *iam.DeletePolicyInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	return &i.deletePolicyOutput, i.deletePolicyErr
}

func (i *mockIAMClient) DeleteRole(ctx context.Context, params *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	return nil, nil
}

func (i *mockIAMClient) DeletePolicyVersion(ctx context.Context, params *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	if i.deletePolicyVersionErr != nil {
		return nil, i.deletePolicyVersionErr
	}
	i.deletedPolicyVersionInputs = append(i.deletedPolicyVersionInputs, params)
	return &i.deletePolicyVersionOutput, nil
}

func (i *mockIAMClient) DeleteUser(ctx context.Context, params *iam.DeleteUserInput, optFns ...func(*iam.Options)) (*iam.DeleteUserOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	return nil, i.deleteUserErr
}

func (i *mockIAMClient) DetachRolePolicy(ctx context.Context, params *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	return nil, nil
}

func (i *mockIAMClient) DetachUserPolicy(ctx context.Context, params *iam.DetachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	if i.policyArn != "" && i.policyArn != *params.PolicyArn {
		return nil, errors.New("unexpected policy ARN")
	}
	return nil, i.detachUserPolicyErr
}

func (i *mockIAMClient) GetPolicy(ctx context.Context, params *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	return &iam.GetPolicyOutput{
		Policy: &types.Policy{
			Arn:              params.PolicyArn,
			DefaultVersionId: aws.String("old"),
		},
	}, nil
}

func (i *mockIAMClient) GetPolicyVersion(ctx context.Context, params *iam.GetPolicyVersionInput, optFns ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	return &iam.GetPolicyVersionOutput{
		PolicyVersion: &types.PolicyVersion{
			Document:         aws.String(mockPolDoc),
			IsDefaultVersion: true,
			VersionId:        params.VersionId,
		},
	}, nil
}

func (i *mockIAMClient) GetRole(ctx context.Context, params *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	arn := "arn:aws:iam::123456789012:role/" + *(params.RoleName)
	return &iam.GetRoleOutput{
		Role: &types.Role{
			Arn:      aws.String(arn),
			RoleName: params.RoleName,
		},
	}, nil
}

func (i *mockIAMClient) GetUser(ctx context.Context, params *iam.GetUserInput, optFns ...func(*iam.Options)) (*iam.GetUserOutput, error) {
	if i.userName != "" && i.userName == *params.UserName {
		return i.getUserOutput, i.getUserErr
	}
	return nil, i.getUserErr
}

func (i *mockIAMClient) ListAccessKeys(ctx context.Context, params *iam.ListAccessKeysInput, optFns ...func(*iam.Options)) (*iam.ListAccessKeysOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	return &i.listAccessKeysOutput, i.listAccessKeysErr
}

func (i *mockIAMClient) ListAttachedRolePolicies(ctx context.Context, params *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	return &iam.ListAttachedRolePoliciesOutput{
		AttachedPolicies: i.attachedRolePolicies,
	}, nil
}

func (i *mockIAMClient) ListAttachedUserPolicies(ctx context.Context, params *iam.ListAttachedUserPoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedUserPoliciesOutput, error) {
	if i.userName != "" && i.userName != *params.UserName {
		return nil, errors.New("unexpected username")
	}
	if i.iamPath != "" && i.iamPath != *params.PathPrefix {
		return nil, errors.New("unexpected username")
	}
	return &i.listAttachedUserPoliciesOutput, i.listAttachedUserPoliciesErr
}

func (i *mockIAMClient) ListPolicyVersions(ctx context.Context, params *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	return &i.listPolicyVersionsOutput, i.listPolicyVersionsErr
}
