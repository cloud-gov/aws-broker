package iampolicy

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
)

var mockPolDoc string = `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": ["some:action"],"Resource": ["some:resource"]}]}`

var bucketArn string = "arn:aws-us-gov:s3:::test"

var existStatement PolicyStatementEntry = PolicyStatementEntry{
	Action:   []string{"some:action"},
	Effect:   "Allow",
	Resource: []string{"some:resource"},
}

var listStatement PolicyStatementEntry = PolicyStatementEntry{
	Action:   []string{"s3:ListBucket"},
	Effect:   "Allow",
	Resource: []string{bucketArn},
}
var objectStatement PolicyStatementEntry = PolicyStatementEntry{
	Action:   []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject"},
	Effect:   "Allow",
	Resource: []string{bucketArn + "/*"},
}

type mockIamClient struct {
	iamiface.IAMAPI

	createRoleErr error
}

func (m *mockIamClient) CreateRole(input *iam.CreateRoleInput) (*iam.CreateRoleOutput, error) {
	if m.createRoleErr != nil {
		return nil, m.createRoleErr
	}
	arn := "arn:aws:iam::123456789012:role/" + *(input.RoleName)
	return &iam.CreateRoleOutput{
		Role: &iam.Role{
			Arn:                      aws.String(arn),
			RoleName:                 input.RoleName,
			AssumeRolePolicyDocument: input.AssumeRolePolicyDocument,
		},
	}, nil
}

func (m *mockIamClient) GetRole(input *iam.GetRoleInput) (*iam.GetRoleOutput, error) {
	arn := "arn:aws:iam::123456789012:role/" + *(input.RoleName)
	return &iam.GetRoleOutput{
		Role: &iam.Role{
			Arn:      aws.String(arn),
			RoleName: input.RoleName,
		},
	}, nil
}

func (m *mockIamClient) CreatePolicy(input *iam.CreatePolicyInput) (*iam.CreatePolicyOutput, error) {
	arn := "arn:aws:iam::123456789012:policy/" + *(input.PolicyName)
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

func (m *mockIamClient) ListPolicyVersions(*iam.ListPolicyVersionsInput) (*iam.ListPolicyVersionsOutput, error) {
	return &iam.ListPolicyVersionsOutput{
		IsTruncated: aws.Bool(false),
		Marker:      aws.String("foobar"),
		Versions: []*iam.PolicyVersion{
			{
				VersionId:        aws.String("old"),
				Document:         aws.String(mockPolDoc),
				IsDefaultVersion: aws.Bool(true),
			},
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
		t.Error("Role is nil")
	}
}

func TestCreateAssumeRoleAlreadyExists(t *testing.T) {
	policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
	rolename := "test-role"
	createRoleErr := awserr.New(iam.ErrCodeEntityAlreadyExistsException, "already exists", errors.New("fail"))
	ip := &IamPolicyHandler{
		iamsvc: &mockIamClient{
			createRoleErr: createRoleErr,
		},
	}

	role, err := ip.CreateAssumeRole(policy, rolename)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if *(role.RoleName) != rolename {
		t.Errorf("RoleName returned as %v, expected: %s", role.RoleName, rolename)
	}
	expectedArn := "arn:aws:iam::123456789012:role/" + rolename
	if *role.Arn != expectedArn {
		t.Errorf("ARN returned as %v, expected %s", role.Arn, expectedArn)
	}
}

func TestCreateUserPolicy(t *testing.T) {
	ip := &IamPolicyHandler{
		iamsvc: &mockIamClient{},
	}
	Domain := "Test"
	ARN := "arn:aws:iam::123456789012:elasticsearch/" + Domain
	snapshotRoleARN := "arn:aws:iam::123456789012:role/test-role"
	policy := `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "iam:PassRole","Resource": "` + snapshotRoleARN + `"},{"Effect": "Allow","Action": "es:ESHttpPut","Resource": "` + ARN + `/*"}]}`
	policyname := Domain + "-to-S3-ESRolePolicy"
	username := Domain
	expectedarn := "arn:aws:iam::123456789012:policy/" + policyname
	policyarn, err := ip.CreateUserPolicy(policy, policyname, username)
	if err != nil {
		t.Error(err)
	}
	if policyarn != "" {
		if policyarn != expectedarn {
			t.Errorf("Expected Arn %s but got %s", expectedarn, policyarn)
		}
	} else {
		t.Error("PolicyARN is nil")
	}
}

func TestCreatePolicyAttachRole(t *testing.T) {
	ip := &IamPolicyHandler{
		iamsvc: &mockIamClient{},
	}
	role := iam.Role{
		RoleName: aws.String("test-role"),
	}
	policyname := "test-pol"
	expectedarn := "arn:aws:iam::123456789012:policy/" + policyname
	policyarn, err := ip.CreatePolicyAttachRole(policyname, mockPolDoc, role)
	if err != nil {
		t.Error(err)
	}
	if policyarn != "" {
		if policyarn != expectedarn {
			t.Errorf("Expected Arn %s but got %s", expectedarn, policyarn)
		}
	} else {
		t.Error("policy arn is nil")
	}

}

func TestUpdateExistingPolicy(t *testing.T) {
	ip := &IamPolicyHandler{
		iamsvc: &mockIamClient{},
	}
	ps := []PolicyStatementEntry{listStatement, existStatement, objectStatement}
	arn := "arn:aws:iam::123456789012:policy/test-pol"
	pd, err := ip.UpdateExistingPolicy(arn, ps)

	if err != nil {
		t.Error(err)
	}
	if pd != nil {
		t.Logf("%+v", *pd)
		var poldoc PolicyDocument
		err = poldoc.FromString(*(pd.Document))
		if err != nil {
			t.Error(err)
		}
		if len(poldoc.Statement) != 3 {
			t.Errorf("expected 3 statements but got %d", len(poldoc.Statement))
		}
	} else {
		t.Error("policy version is nil")
	}

}
