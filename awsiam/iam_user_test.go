package awsiam

import (
	"errors"
	"testing"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-test/deep"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
)

var (
	testSink = lagertest.NewTestSink()
	iamPath  = "/"
	userName = "iam-user"
)

func TestMain(m *testing.M) {
	logger.RegisterSink(testSink)
}

func TestDescribeIAMUser(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
	}, logger)

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
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
		getUserErr: errors.New("operation failed"),
	}, logger)

	_, err := iamUserClient.Describe(userName)
	if err == nil {
		t.Fatal("expected error but received none")
	}

	if err.Error() != "operation failed" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDescribeIAMUserAWSError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
		getUserErr: &types.AccessDeniedException{
			Message: aws.String("access denied"),
		},
	}, logger)

	_, err := iamUserClient.Describe(userName)

	var accessDeniedException *types.AccessDeniedException
	if !errors.As(err, &accessDeniedException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCreateUser(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		createUserOutput: &iam.CreateUserOutput{
			User: &iamTypes.User{
				Arn: aws.String("user-arn"),
			},
		},
	}, logger)

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
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:      userName,
		createUserErr: &iamTypes.EntityAlreadyExistsException{},
	}, logger)

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
	iamUserClient := NewIAMUserClient(&MockIAMClient{}, logger)

	err := iamUserClient.Delete(userName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteUserError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		deleteUserErr: &iamTypes.DeleteConflictException{},
	}, logger)

	err := iamUserClient.Delete(userName)
	var deleteConflictException *iamTypes.DeleteConflictException
	if !errors.As(err, &deleteConflictException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestListAccessKeys(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
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
	}, logger)

	accessKeys, err := iamUserClient.ListAccessKeys(userName)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(accessKeys, []string{"access-key-id-1", "access-key-id-2"}); diff != nil {
		t.Error(diff)
	}
}

func TestListAccessKeysError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:          userName,
		listAccessKeysErr: &iamTypes.NoSuchEntityException{},
	}, logger)

	_, err := iamUserClient.ListAccessKeys(userName)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCreateAccessKey(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		createAccessKeyOutput: iam.CreateAccessKeyOutput{
			AccessKey: &iamTypes.AccessKey{
				AccessKeyId:     aws.String("access-key-id"),
				SecretAccessKey: aws.String("secret-access-key"),
			},
		},
	}, logger)

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
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:           userName,
		createAccessKeyErr: &iamTypes.NoSuchEntityException{},
	}, logger)

	_, _, err := iamUserClient.CreateAccessKey(userName)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDeleteAccessKey(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
	}, logger)
	err := iamUserClient.DeleteAccessKey(userName, "access-key-id")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteAccessKeyError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:           userName,
		deleteAccessKeyErr: &iamTypes.NoSuchEntityException{},
	}, logger)
	err := iamUserClient.DeleteAccessKey(userName, "access-key-id")
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestListAttachedUserPolicies(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
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
	}, logger)
	policies, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(policies, []string{"user-policy-1", "user-policy-2"}); diff != nil {
		t.Error(diff)
	}
}

func TestListAttachedUserPoliciesError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:                    userName,
		iamPath:                     iamPath,
		listAttachedUserPoliciesErr: &iamTypes.NoSuchEntityException{},
	}, logger)
	_, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestAttachUserPolicy(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:  userName,
		policyArn: "policy-arn",
	}, logger)
	err := iamUserClient.AttachUserPolicy(userName, "policy-arn")
	if err != nil {
		t.Fatal(err)
	}
}

func TestAttachUserPolicyError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:            userName,
		policyArn:           "policy-arn",
		attachUserPolicyErr: &iamTypes.NoSuchEntityException{},
	}, logger)
	err := iamUserClient.AttachUserPolicy(userName, "policy-arn")
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDetachUserPolicy(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:  userName,
		policyArn: "policy-arn",
	}, logger)
	err := iamUserClient.DetachUserPolicy(userName, "policy-arn")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDetachUserPolicyError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:            userName,
		policyArn:           "policy-arn",
		detachUserPolicyErr: &iamTypes.NoSuchEntityException{},
	}, logger)
	err := iamUserClient.DetachUserPolicy(userName, "policy-arn")
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}
