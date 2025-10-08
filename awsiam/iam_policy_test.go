package awsiam

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/go-test/deep"
)

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

func TestCreateAssumeRole(t *testing.T) {
	policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
	rolename := "test-role"
	ip := NewIAMPolicyClient(&MockIAMClient{}, nil)
	iamTags := []types.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}

	role, _ := ip.CreateAssumeRole(policy, rolename, iamTags)
	if role != nil {
		if *(role.RoleName) != rolename {
			t.Errorf("RoleName returned as %v", role.RoleName)
		}

		if diff := deep.Equal(role.Tags, iamTags); diff != nil {
			t.Error(diff)
		}
	} else {
		t.Error("Role is nil")
	}
}

func TestCreateAssumeRoleAlreadyExists(t *testing.T) {
	policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
	rolename := "test-role"

	ip := NewIAMPolicyClient(&MockIAMClient{
		createRoleErr: &types.EntityAlreadyExistsException{
			Message: aws.String("fail"),
		},
	}, nil)

	role, err := ip.CreateAssumeRole(policy, rolename, nil)
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
	ip := NewIAMPolicyClient(&MockIAMClient{}, nil)
	Domain := "Test"
	ARN := "arn:aws:iam::123456789012:elasticsearch/" + Domain
	snapshotRoleARN := "arn:aws:iam::123456789012:role/test-role"
	policy := `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "iam:PassRole","Resource": "` + snapshotRoleARN + `"},{"Effect": "Allow","Action": "es:ESHttpPut","Resource": "` + ARN + `/*"}]}`
	policyname := Domain + "-to-S3-ESRolePolicy"
	username := Domain
	expectedarn := "arn:aws:iam::123456789012:policy/" + policyname
	policyarn, err := ip.CreateUserPolicy(policy, policyname, username, nil)
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

	ip := NewIAMPolicyClient(&MockIAMClient{

		listAttachedUserPoliciesOutput: iam.ListAttachedUserPoliciesOutput{
			AttachedPolicies: []types.AttachedPolicy{
				{
					PolicyArn:  aws.String("arn:aws:iam::123456789012:policy/" + policyname),
					PolicyName: aws.String(policyname),
				},
			},
		},
		createPolicyErr: &types.EntityAlreadyExistsException{
			Message: aws.String("policy already exists"),
		},
	}, nil)

	policyArn, err := ip.CreateUserPolicy(policy, policyname, username, nil)
	if err != nil {
		t.Fatal(err)
	}

	expectedArn := "arn:aws:iam::123456789012:policy/" + policyname
	if policyArn != expectedArn {
		t.Errorf("Expected Arn %s but got %s", expectedArn, policyArn)
	}
}

func TestCreatePolicyAttachRole(t *testing.T) {
	ip := NewIAMPolicyClient(&MockIAMClient{}, nil)
	role := types.Role{
		RoleName: aws.String("test-role"),
	}
	policyname := "test-pol"
	expectedarn := "arn:aws:iam::123456789012:policy/" + policyname
	policyarn, err := ip.CreatePolicyAttachRole(policyname, mockPolDoc, role, nil)
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

	ip := NewIAMPolicyClient(&MockIAMClient{
		attachedRolePolicies: []types.AttachedPolicy{
			{
				PolicyName: aws.String(policyName),
				PolicyArn:  aws.String("arn:aws:iam::123456789012:policy/" + policyName),
			},
		},
		createPolicyErr: &types.EntityAlreadyExistsException{
			Message: aws.String("policy already exists"),
		},
	}, nil)

	role := types.Role{
		RoleName: aws.String(roleName),
	}

	policyarn, err := ip.CreatePolicyAttachRole(policyName, mockPolDoc, role, nil)
	if err != nil {
		t.Fatal(err)
	}

	expectedarn := "arn:aws:iam::123456789012:policy/" + policyName
	if policyarn != expectedarn {
		t.Errorf("Expected Arn %s but got %s", expectedarn, policyarn)
	}
}

func TestUpdateExistingPolicy(t *testing.T) {
	ip := NewIAMPolicyClient(&MockIAMClient{
		listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
			Versions: []types.PolicyVersion{
				{VersionId: aws.String("1"), IsDefaultVersion: true},
			},
		},
	}, logger)

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
			iamPolicyClient: NewIAMPolicyClient(&MockIAMClient{
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []types.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: true},
					},
				},
			}, logger),
		},
		"returns delete policy error": {
			policyArn: "arn2",
			iamPolicyClient: NewIAMPolicyClient(&MockIAMClient{
				deletePolicyErr: errors.New("delete policy version error"),
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []types.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: true},
					},
				},
			}, logger),
			expectedErrMessage: "delete policy version error",
		},
		"returns an AWS error": {
			policyArn: "arn2",
			iamPolicyClient: NewIAMPolicyClient(&MockIAMClient{
				deletePolicyErr: &types.NoSuchEntityException{
					Message: aws.String("not found"),
				},
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []types.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: true},
					},
				},
			}, logger),
			expectedErrMessage: "not found",
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
		fakeIAMClient                     *MockIAMClient
		expectedErr                       error
		expectedDeletePolicyVersionInputs []*iam.DeletePolicyVersionInput
	}{
		"deletes non-default policy versions": {
			policyArn: policyArn,
			fakeIAMClient: &MockIAMClient{
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []types.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: true},
						{VersionId: aws.String("2"), IsDefaultVersion: false},
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
			fakeIAMClient: &MockIAMClient{
				listPolicyVersionsErr: listPolicyVersionsErr,
			},
			expectedErr: listPolicyVersionsErr,
		},
		"error deleting policy version": {
			policyArn: policyArn,
			fakeIAMClient: &MockIAMClient{
				listPolicyVersionsOutput: iam.ListPolicyVersionsOutput{
					Versions: []types.PolicyVersion{
						{VersionId: aws.String("1"), IsDefaultVersion: true},
						{VersionId: aws.String("2"), IsDefaultVersion: false},
					},
				},
				deletePolicyVersionErr: deletePolicyVersionErr,
			},
			expectedErr: deletePolicyVersionErr,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			iamPolicyClient := NewIAMPolicyClient(test.fakeIAMClient, logger)
			err := iamPolicyClient.deleteNonDefaultPolicyVersions(test.policyArn)
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if !reflect.DeepEqual(
				test.fakeIAMClient.deletedPolicyVersionInputs,
				test.expectedDeletePolicyVersionInputs,
			) {
				t.Errorf("expected: %+v, got: %+v", test.expectedDeletePolicyVersionInputs, test.fakeIAMClient.deletedPolicyVersionInputs)
			}
		})
	}
}

func TestCreatePolicyFromTemplate(t *testing.T) {
	testCases := map[string]struct {
		fakeIAMClient              *MockIAMClient
		policyArn                  string
		expectedErrMessage         string
		iamPolicyClient            *IAMPolicyClient
		policyName                 string
		policyTemplate             string
		resources                  []string
		iamPath                    string
		expectedPolicyArn          string
		expectedCreatePolicyInputs []*iam.CreatePolicyInput
		iamTags                    []types.Tag
	}{
		"creates policy": {
			fakeIAMClient:     &MockIAMClient{},
			policyName:        "policy-name",
			policyTemplate:    `{"Version": "2012-10-17","Id": "policy-name","Statement": [{"Action":"action","Effect":"effect","Resource": {{resources "/*"}}}]}`,
			resources:         []string{"resource"},
			iamPath:           "/path/",
			expectedPolicyArn: "arn:aws:iam::123456789012:policy/policy-name",
			iamTags: []types.Tag{
				{
					Key:   aws.String("foo"),
					Value: aws.String("bar"),
				},
			},
			expectedCreatePolicyInputs: []*iam.CreatePolicyInput{
				{
					PolicyName:     aws.String("policy-name"),
					PolicyDocument: aws.String(`{"Version": "2012-10-17","Id": "policy-name","Statement": [{"Action":"action","Effect":"effect","Resource": ["resource/*"]}]}`),
					Path:           aws.String("/path/"),
					Tags: []types.Tag{
						{
							Key:   aws.String("foo"),
							Value: aws.String("bar"),
						},
					},
				},
			},
		},
		"returns error": {
			fakeIAMClient: &MockIAMClient{
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
			fakeIAMClient: &MockIAMClient{
				createPolicyErr: &types.EntityAlreadyExistsException{
					Message: aws.String("already exists"),
				},
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
			expectedErrMessage: "already exists",
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
				test.iamTags,
			)
			if test.expectedErrMessage != "" && err.Error() != test.expectedErrMessage {
				t.Fatalf("expected error message: %s, got: %s", test.expectedErrMessage, err.Error())
			}
			if policyARN != test.expectedPolicyArn {
				t.Fatalf("unexpected policy ARN: %s", policyARN)
			}
			if diff := deep.Equal(test.fakeIAMClient.createPolicyInputs, test.expectedCreatePolicyInputs); diff != nil {
				t.Error(diff)
			}
		})
	}
}
