package awsiam

import (
	"errors"
	"testing"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go-v2/aws"

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
