package awsiam

import (
	"errors"
	"testing"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-test/deep"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

var (
	testSink = lagertest.NewTestSink()
	iamPath  = "/"
	userName = "iam-user"
)

func NewTestIAMUserClient(iamSvc IAMClientInterface) *IAMUserClient {
	logger.RegisterSink(testSink)
	return NewIAMUserClient(iamSvc, logger)
}

func TestDescribeIAMUser(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
	})

	userDetails, err := iamUserClient.Describe(userName)
	if err != nil {
		t.Fatal(err)
	}

	properUserDetails := UserDetails{
		UserName: userName,
		UserARN:  "user-arn",
		UserID:   "user-id",
	}
	if diff := deep.Equal(userDetails, properUserDetails); diff != nil {
		t.Error(diff)
	}
}

func TestDescribeIAMUserError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
		getUserErr: errors.New("operation failed"),
	})

	_, err := iamUserClient.Describe(userName)
	if err == nil {
		t.Fatal("expected error but received none")
	}

	if err.Error() != "operation failed" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDescribeIAMUserAWSError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
		getUserErr: &iamTypes.NoSuchEntityException{
			Message: aws.String("not found"),
		},
	})

	_, err := iamUserClient.Describe(userName)

	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCreateUser(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		createUserOutput: &iam.CreateUserOutput{
			User: &iamTypes.User{
				Arn: aws.String("user-arn"),
			},
		},
	})

	iamTags := []iamTypes.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}

	userArn, err := iamUserClient.Create(userName, iamPath, iamTags)
	if err != nil {
		t.Fatal(err)
	}

	if userArn != "user-arn" {
		t.Fatalf("unexpected user ARN: %s", userArn)
	}
}

func TestCreateUserError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:      userName,
		createUserErr: &iamTypes.EntityAlreadyExistsException{},
	})

	iamTags := []iamTypes.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}

	_, err := iamUserClient.Create(userName, iamPath, iamTags)

	var accessDeniedException *iamTypes.EntityAlreadyExistsException
	if !errors.As(err, &accessDeniedException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDeleteUser(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{})

	err := iamUserClient.Delete(userName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteUserError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		deleteUserErr: &iamTypes.DeleteConflictException{},
	})

	err := iamUserClient.Delete(userName)
	var deleteConflictException *iamTypes.DeleteConflictException
	if !errors.As(err, &deleteConflictException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestListAccessKeys(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		listAccessKeysOutput: iam.ListAccessKeysOutput{
			AccessKeyMetadata: []iamTypes.AccessKeyMetadata{
				{
					AccessKeyId: aws.String("access-key-id-1"),
				},
				{
					AccessKeyId: aws.String("access-key-id-2"),
				},
			},
		},
	})

	accessKeys, err := iamUserClient.ListAccessKeys(userName)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(accessKeys, []string{"access-key-id-1", "access-key-id-2"}); diff != nil {
		t.Error(diff)
	}
}

func TestListAccessKeysError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:          userName,
		listAccessKeysErr: &iamTypes.NoSuchEntityException{},
	})

	_, err := iamUserClient.ListAccessKeys(userName)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCreateAccessKey(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		createAccessKeyOutput: iam.CreateAccessKeyOutput{
			AccessKey: &iamTypes.AccessKey{
				AccessKeyId:     aws.String("access-key-id"),
				SecretAccessKey: aws.String("secret-access-key"),
			},
		},
	})

	accessKeyID, secretAccessKey, err := iamUserClient.CreateAccessKey(userName)
	if err != nil {
		t.Fatal(err)
	}

	if accessKeyID != "access-key-id" {
		t.Fatalf("unexpected access key: %s", accessKeyID)
	}

	if secretAccessKey != "secret-access-key" {
		t.Fatalf("unexpected secret key: %s", secretAccessKey)
	}
}

func TestCreateAccessKeyError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:           userName,
		createAccessKeyErr: &iamTypes.NoSuchEntityException{},
	})

	_, _, err := iamUserClient.CreateAccessKey(userName)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDeleteAccessKey(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
	})
	err := iamUserClient.DeleteAccessKey(userName, "access-key-id")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteAccessKeyError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:           userName,
		deleteAccessKeyErr: &iamTypes.NoSuchEntityException{},
	})
	err := iamUserClient.DeleteAccessKey(userName, "access-key-id")
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestListAttachedUserPolicies(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName: userName,
		iamPath:  iamPath,
		listAttachedUserPoliciesOutput: iam.ListAttachedUserPoliciesOutput{
			AttachedPolicies: []iamTypes.AttachedPolicy{
				{
					PolicyArn: aws.String("user-policy-1"),
				},
				{
					PolicyArn: aws.String("user-policy-2"),
				},
			},
		},
	})
	policies, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(policies, []string{"user-policy-1", "user-policy-2"}); diff != nil {
		t.Error(diff)
	}
}

func TestListAttachedUserPoliciesError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:                    userName,
		iamPath:                     iamPath,
		listAttachedUserPoliciesErr: &iamTypes.NoSuchEntityException{},
	})
	_, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestAttachUserPolicy(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:  userName,
		policyArn: "policy-arn",
	})
	err := iamUserClient.AttachUserPolicy(userName, "policy-arn")
	if err != nil {
		t.Fatal(err)
	}
}

func TestAttachUserPolicyError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:            userName,
		policyArn:           "policy-arn",
		attachUserPolicyErr: &iamTypes.NoSuchEntityException{},
	})
	err := iamUserClient.AttachUserPolicy(userName, "policy-arn")
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDetachUserPolicy(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:  userName,
		policyArn: "policy-arn",
	})
	err := iamUserClient.DetachUserPolicy(userName, "policy-arn")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDetachUserPolicyError(t *testing.T) {
	iamUserClient := NewTestIAMUserClient(&mockIAMClient{
		userName:            userName,
		policyArn:           "policy-arn",
		detachUserPolicyErr: &iamTypes.NoSuchEntityException{},
	})
	err := iamUserClient.DetachUserPolicy(userName, "policy-arn")
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}
