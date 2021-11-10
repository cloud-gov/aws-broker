package iampolicy

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
)

var mockPolDoc string = `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": ["some:action"],"Resource": ["some:resource"]"}]}`

type mockIamClient struct {
	iamiface.IAMAPI
}

func (m *mockIamClient) CreateRole(input *iam.CreateRoleInput) (*iam.CreateRoleOutput, error) {
	arn := "arn:aws:iam::123456789012:role/" + *input.RoleName + *input.Path
	return &iam.CreateRoleOutput{
		Role: &iam.Role{
			Arn:                      aws.String(arn),
			RoleName:                 input.RoleName,
			AssumeRolePolicyDocument: input.AssumeRolePolicyDocument,
		},
	}, nil
}

func (m *mockIamClient) CreatePolicy(input *iam.CreatePolicyInput) (*iam.CreatePolicyOutput, error) {
	arn := "arn:aws:iam::123456789012:policy/" + *input.PolicyName + *input.Path
	return &iam.CreatePolicyOutput{
		Policy: &iam.Policy{
			Arn:        aws.String(arn),
			PolicyName: input.PolicyName,
		},
	}, nil
}

func (m *mockIamClient) AttachUserPolicy(input *iam.AttachUserPolicyInput) (*iam.AttachUserPolicyOutput, error) {
	return &iam.AttachUserPolicyOutput{}, nil
}

func (m *mockIamClient) AttachRolePolicy(input *iam.AttachRolePolicyInput) (*iam.AttachRolePolicyOutput, error) {
	return &iam.AttachRolePolicyOutput{}, nil
}

func (m *mockIamClient) GetPolicy(input *iam.GetPolicyInput) (*iam.GetPolicyOutput, error) {
	return &iam.GetPolicyOutput{
		Policy: &iam.Policy{
			Arn:              input.PolicyArn,
			DefaultVersionId: aws.String("old"),
		},
	}, nil
}

func (m *mockIamClient) GetPolicyVersion(input *iam.GetPolicyVersionInput) (*iam.GetPolicyVersionOutput, error) {
	return &iam.GetPolicyVersionOutput{
		PolicyVersion: &iam.PolicyVersion{
			Document:         aws.String(mockPolDoc),
			IsDefaultVersion: aws.Bool(true),
			VersionId:        input.VersionId,
		},
	}, nil
}

func (m *mockIamClient) CreatePolicyVersion(input *iam.CreatePolicyVersionInput) (*iam.CreatePolicyVersionOutput, error) {
	return &iam.CreatePolicyVersionOutput{
		PolicyVersion: &iam.PolicyVersion{
			VersionId:        aws.String("new"),
			Document:         input.PolicyDocument,
			IsDefaultVersion: input.SetAsDefault,
		},
	}, nil
}

func TestCreateAssumeRole(t *testing.T) {
	policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
	rolename := "test-role"
	ip := &IamPolicyHandler{
		iamsvc: &mockIamClient{},
	}

	role, _ := ip.CreateAssumeRole(policy, rolename)
	if role != nil {
		if *(role.RoleName) != rolename {
			t.Errorf("RoleName returned as %v", role.RoleName)
		}
	} else {
		t.Failed()
	}
}

func TestCreateUserPolicy(t *testing.T) {}

func TestCreatePolicyAttachRole(t *testing.T) {}

func TestUpdateExistingPolicy(t *testing.T) {}
