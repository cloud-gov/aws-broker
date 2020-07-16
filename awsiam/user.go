package awsiam

import (
	"errors"
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
