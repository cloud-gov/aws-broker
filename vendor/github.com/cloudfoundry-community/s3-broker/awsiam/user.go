package awsiam

import (
	"errors"
	"fmt"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
)

type User interface {
	Describe(userName string) (UserDetails, error)
	Create(userName, iamPath string) (string, error)
	Delete(userName string) error
	ListAccessKeys(userName string) ([]string, error)
	CreateAccessKey(userName string) (string, string, error)
	DeleteAccessKey(userName, accessKeyID string) error
	CreatePolicy(policyName, iamPath, policyTemplate string, resources []string) (string, error)
	DeletePolicy(policyARN string) error
	ListAttachedUserPolicies(userName, iamPath string) ([]string, error)
	AttachUserPolicy(userName, policyARN string) error
	DetachUserPolicy(userName, policyARN string) error
}

type UserDetails struct {
	UserName string
	UserARN  string
	UserID   string
}

var (
	ErrUserDoesNotExist = errors.New("iam user does not exist")
)

func NewUser(provider string, logger lager.Logger, awsSession *session.Session, endpoint string, insecureSkipVerify bool) (User, error) {
	var user User
	if provider == "minio" {
		fmt.Printf("Setting up MinIO user provider...\n")
		awscreds, err := awsSession.Config.Credentials.Get()
		if err != nil {
			return nil, fmt.Errorf("Failure to pull AWS credentials: %v", err)
		}
		user = NewMinioUser(logger, endpoint, awscreds.AccessKeyID, awscreds.SecretAccessKey, insecureSkipVerify, awsSession.Config.HTTPClient.Transport)
	} else if provider == "" || provider == "aws" {
		fmt.Printf("Setting up AWS IAM user provider...\n")
		iamsvc := iam.New(awsSession)
		user = NewIAMUser(iamsvc, logger)
	} else {
		return nil, fmt.Errorf("Unknown provider type: %s", provider)
	}
	return user, nil
}
