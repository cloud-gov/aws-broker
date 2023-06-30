package awsiam

import (
	"errors"
	"reflect"
	"testing"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
)

var mockPolDoc string = `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": ["some:action"],"Resource": ["some:resource"]}]}`

var bucketArn string = "arn:aws-us-gov:s3:::test"

var logger lager.Logger = lager.NewLogger("aws-broker")

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

	createRoleErr      error
	createPolicyErr    error
	createPolicyInputs []*iam.CreatePolicyInput

	attachedUserPolicies []*iam.AttachedPolicy
	attachedRolePolicies []*iam.AttachedPolicy

	listPolicyVersionsOutput iam.ListPolicyVersionsOutput
	listPolicyVersionsErr    error

	deletePolicyOutput iam.DeletePolicyOutput
	deletePolicyErr    error

	deletePolicyVersionOutput  iam.DeletePolicyVersionOutput
	deletePolicyVersionErr     error
	deletedPolicyVersionInputs []*iam.DeletePolicyVersionInput
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
	if m.createPolicyErr != nil {
		return nil, m.createPolicyErr
	}
	m.createPolicyInputs = append(m.createPolicyInputs, input)
	arn := "arn:aws:iam::123456789012:policy/" + *(input.PolicyName)
	return &iam.CreatePolicyOutput{
		Policy: &iam.Policy{
			Arn:        aws.String(arn),
			PolicyName: input.PolicyName,
		},
	}, nil
}

func (m *mockIamClient) ListAttachedUserPolicies(input *iam.ListAttachedUserPoliciesInput) (*iam.ListAttachedUserPoliciesOutput, error) {
	return &iam.ListAttachedUserPoliciesOutput{
		AttachedPolicies: m.attachedUserPolicies,
	}, nil
}

func (m *mockIamClient) ListAttachedRolePolicies(input *iam.ListAttachedRolePoliciesInput) (*iam.ListAttachedRolePoliciesOutput, error) {
	return &iam.ListAttachedRolePoliciesOutput{
		AttachedPolicies: m.attachedRolePolicies,
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

func (m *mockIamClient) ListPolicyVersions(input *iam.ListPolicyVersionsInput) (*iam.ListPolicyVersionsOutput, error) {
	return &m.listPolicyVersionsOutput, m.listPolicyVersionsErr
}

func (m *mockIamClient) DeletePolicyVersion(input *iam.DeletePolicyVersionInput) (
	*iam.DeletePolicyVersionOutput,
	error,
) {
	if m.deletePolicyVersionErr != nil {
		return nil, m.deletePolicyVersionErr
	}
	m.deletedPolicyVersionInputs = append(m.deletedPolicyVersionInputs, input)
	return &m.deletePolicyVersionOutput, nil
}

func (m *mockIamClient) DeletePolicy(input *iam.DeletePolicyInput) (*iam.DeletePolicyOutput, error) {
	return &m.deletePolicyOutput, m.deletePolicyErr
}

func TestCreateAssumeRole(t *testing.T) {
	policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
	rolename := "test-role"
	ip := &IAMPolicyClient{
		iam: &mockIamClient{},
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
	ip := &IAMPolicyClient{
		iam: &mockIamClient{
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
	ip := &IAMPolicyClient{
		iam: &mockIamClient{},
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

func TestCreateUserPolicyAlreadyExists(t *testing.T) {
	Domain := "foobar"
	ARN := "arn:aws:iam::123456789012:elasticsearch/" + Domain
	snapshotRoleARN := "arn:aws:iam::123456789012:role/test-role"
	policy := `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "iam:PassRole","Resource": "` + snapshotRoleARN + `"},{"Effect": "Allow","Action": "es:ESHttpPut","Resource": "` + ARN + `/*"}]}`
	policyname := Domain + "-to-S3-ESRolePolicy"
	username := Domain

	createPolicyErr := awserr.New(iam.ErrCodeEntityAlreadyExistsException, "policy already exists", errors.New("fail"))

	ip := &IAMPolicyClient{
		iam: &mockIamClient{
			createPolicyErr: createPolicyErr,
			attachedUserPolicies: []*iam.AttachedPolicy{
				{
					PolicyArn:  aws.String("arn:aws:iam::123456789012:policy/" + policyname),
					PolicyName: aws.String(policyname),
				},
			},
		},
	}

	policyArn, err := ip.CreateUserPolicy(policy, policyname, username)
	if err != nil {
		t.Fatal(err)
	}

	expectedArn := "arn:aws:iam::123456789012:policy/" + policyname
	if policyArn != expectedArn {
		t.Errorf("Expected Arn %s but got %s", expectedArn, policyArn)
	}
}

func TestCreatePolicyAttachRole(t *testing.T) {
	ip := &IAMPolicyClient{
		iam: &mockIamClient{},
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

func TestCreatePolicyAttachRoleAlreadyExists(t *testing.T) {
	policyName := "test-pol"
	roleName := "test-role"
	createPolicyErr := awserr.New(iam.ErrCodeEntityAlreadyExistsException, "policy already exists", errors.New("fail"))

	ip := &IAMPolicyClient{
		iam: &mockIamClient{
			createPolicyErr: createPolicyErr,
			attachedRolePolicies: []*iam.AttachedPolicy{
				{
					PolicyName: aws.String(policyName),
					PolicyArn:  aws.String("arn:aws:iam::123456789012:policy/" + policyName),
				},
			},
		},
	}
	role := iam.Role{
		RoleName: aws.String(roleName),
	}

	policyarn, err := ip.CreatePolicyAttachRole(policyName, mockPolDoc, role)
	if err != nil {
		t.Fatal(err)
	}

	expectedarn := "arn:aws:iam::123456789012:policy/" + policyName
	if policyarn != expectedarn {
		t.Errorf("Expected Arn %s but got %s", expectedarn, policyarn)
	}
}

func TestUpdateExistingPolicy(t *testing.T) {
	ip := &IAMPolicyClient{
		iam: &mockIamClient{
			listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
				Versions: []*iam.PolicyVersion{
					{VersionId: aws.String("1"), IsDefaultVersion: aws.Bool(true)},
				},
			},
		},
		logger: logger,
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

func TestDeletePolicy(t *testing.T) {
	testCases := map[string]struct {
		policyArn          string
		iamPolicyClient    *IAMPolicyClient
		expectedErr        error
		expectedErrMessage string
	}{
		"success": {
			policyArn: "arn1",
			iamPolicyClient: &IAMPolicyClient{
				iam: &mockIamClient{
					listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
						Versions: []*iam.PolicyVersion{
							{VersionId: aws.String("1"), IsDefaultVersion: aws.Bool(true)},
						},
					},
				},
				logger: logger,
			},
		},
		"returns delete policy error": {
			policyArn: "arn2",
			iamPolicyClient: &IAMPolicyClient{
				iam: &mockIamClient{
					listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
						Versions: []*iam.PolicyVersion{
							{VersionId: aws.String("1"), IsDefaultVersion: aws.Bool(true)},
						},
					},
					deletePolicyErr: errors.New("delete policy version error"),
				},
				logger: logger,
			},
			expectedErrMessage: "delete policy version error",
		},
		"returns an AWS error": {
			policyArn: "arn2",
			iamPolicyClient: &IAMPolicyClient{
				iam: &mockIamClient{
					listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
						Versions: []*iam.PolicyVersion{
							{VersionId: aws.String("1"), IsDefaultVersion: aws.Bool(true)},
						},
					},
					deletePolicyErr: awserr.New("code", "message", errors.New("operation failed")),
				},
				logger: logger,
			},
			expectedErrMessage: "code: message",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.iamPolicyClient.DeletePolicy(test.policyArn)
			if test.expectedErrMessage != "" && err.Error() != test.expectedErrMessage {
				t.Errorf("expected error: %s, got: %s", test.expectedErrMessage, err.Error())
			}
		})
	}
}

func TestDeleteNonDefaultPolicyVersions(t *testing.T) {
	policyArn := "arn1"
	listPolicyVersionsErr := errors.New("list policy versions err")
	deletePolicyVersionErr := errors.New("delete policy version err")

	testCases := map[string]struct {
		policyArn                         string
		fakeIAMClient                     *mockIamClient
		expectedErr                       error
		expectedDeletePolicyVersionInputs []*iam.DeletePolicyVersionInput
	}{
		"deletes non-default policy versions": {
			policyArn: policyArn,
			fakeIAMClient: &mockIamClient{
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []*iam.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: aws.Bool(true)},
						{VersionId: aws.String("2"), IsDefaultVersion: aws.Bool(false)},
					},
				},
			},
			expectedDeletePolicyVersionInputs: []*iam.DeletePolicyVersionInput{
				{
					VersionId: aws.String("2"),
					PolicyArn: &policyArn,
				},
			},
		},
		"error on listing policy versions": {
			policyArn: policyArn,
			fakeIAMClient: &mockIamClient{
				listPolicyVersionsErr: listPolicyVersionsErr,
			},
			expectedErr: listPolicyVersionsErr,
		},
		"error deleting policy version": {
			policyArn: policyArn,
			fakeIAMClient: &mockIamClient{
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []*iam.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: aws.Bool(true)},
						{VersionId: aws.String("2"), IsDefaultVersion: aws.Bool(false)},
					},
				},
				deletePolicyVersionErr: deletePolicyVersionErr,
			},
			expectedErr: deletePolicyVersionErr,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			iamPolicyClient := &IAMPolicyClient{
				iam:    test.fakeIAMClient,
				logger: logger,
			}
			err := iamPolicyClient.deleteNonDefaultPolicyVersions(test.policyArn)
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if !reflect.DeepEqual(
				test.fakeIAMClient.deletedPolicyVersionInputs,
				test.expectedDeletePolicyVersionInputs,
			) {
				t.Errorf("expected: %s, got: %s", test.expectedDeletePolicyVersionInputs, test.fakeIAMClient.deletedPolicyVersionInputs)
			}
		})
	}
}

func TestCreatePolicyFromTemplate(t *testing.T) {
	testCases := map[string]struct {
		fakeIAMClient              *mockIamClient
		policyArn                  string
		expectedErrMessage         string
		iamPolicyClient            *IAMPolicyClient
		policyName                 string
		policyTemplate             string
		resources                  []string
		iamPath                    string
		expectedPolicyArn          string
		expectedCreatePolicyInputs []*iam.CreatePolicyInput
	}{
		"creates policy": {
			fakeIAMClient:     &mockIamClient{},
			policyName:        "policy-name",
			policyTemplate:    `{"Version": "2012-10-17","Id": "policy-name","Statement": [{"Action":"action","Effect":"effect","Resource": {{resources "/*"}}}]}`,
			resources:         []string{"resource"},
			iamPath:           "/path/",
			expectedPolicyArn: "arn:aws:iam::123456789012:policy/policy-name",
			expectedCreatePolicyInputs: []*iam.CreatePolicyInput{
				{
					PolicyName:     aws.String("policy-name"),
					PolicyDocument: aws.String(`{"Version": "2012-10-17","Id": "policy-name","Statement": [{"Action":"action","Effect":"effect","Resource": ["resource/*"]}]}`),
					Path:           aws.String("/path/"),
				},
			},
		},
		"returns error": {
			fakeIAMClient: &mockIamClient{
				createPolicyErr: errors.New("create policy error"),
			},
			policyName: "policy-name",
			policyTemplate: `{
				"Version": "2012-10-17",
				"Id": "policy-name",
				"Statement": [
					{
						"Effect": "effect",
						"Action": "action",
						"Resource": {{resources "/*"}}
					}
				]
			}`,
			resources:          []string{"resource"},
			iamPath:            "/path/",
			expectedErrMessage: "create policy error",
		},
		"returns AWS error": {
			fakeIAMClient: &mockIamClient{
				createPolicyErr: awserr.New("code", "message", errors.New("operation failed")),
			},
			policyName: "policy-name",
			policyTemplate: `{
				"Version": "2012-10-17",
				"Id": "policy-name",
				"Statement": [
					{
						"Effect": "effect",
						"Action": "action",
						"Resource": {{resources "/*"}}
					}
				]
			}`,
			resources:          []string{"resource"},
			iamPath:            "/path/",
			expectedErrMessage: "code: message",
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			iamPolicyClient := &IAMPolicyClient{
				iam:    test.fakeIAMClient,
				logger: logger,
			}
			policyARN, err := iamPolicyClient.CreatePolicyFromTemplate(
				test.policyName,
				test.iamPath,
				test.policyTemplate,
				test.resources,
			)
			if test.expectedErrMessage != "" && err.Error() != test.expectedErrMessage {
				t.Fatalf("expected error message: %s, got: %s", test.expectedErrMessage, err.Error())
			}
			if policyARN != test.expectedPolicyArn {
				t.Fatalf("unexpected policy ARN: %s", policyARN)
			}
			if !reflect.DeepEqual(test.fakeIAMClient.createPolicyInputs, test.expectedCreatePolicyInputs) {
				t.Fatalf("expected: %s, got: %s", test.expectedCreatePolicyInputs, test.fakeIAMClient.createPolicyInputs)
			}
		})
	}
}
