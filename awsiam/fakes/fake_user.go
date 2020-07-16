package fakes

import (
	"github.com/cloudfoundry-community/s3-broker/awsiam"
)

type FakeUser struct {
	DescribeCalled      bool
	DescribeUserName    string
	DescribeUserDetails awsiam.UserDetails
	DescribeError       error

	CreateCalled   bool
	CreateUserName string
	CreateUserARN  string
	CreateError    error

	DeleteCalled   bool
	DeleteUserName string
	DeleteError    error

	ListAccessKeysCalled     bool
	ListAccessKeysUserName   string
	ListAccessKeysAccessKeys []string
	ListAccessKeysError      error

	CreateAccessKeyCalled          bool
	CreateAccessKeyUserName        string
	CreateAccessKeyAccessKeyID     string
	CreateAccessKeySecretAccessKey string
	CreateAccessKeyError           error

	DeleteAccessKeyCalled      bool
	DeleteAccessKeyUserName    string
	DeleteAccessKeyAccessKeyID string
	DeleteAccessKeyError       error

	CreatePolicyCalled     bool
	CreatePolicyPolicyName string
	CreatePolicyEffect     string
	CreatePolicyAction     string
	CreatePolicyResource   string
	CreatePolicyPolicyARN  string
	CreatePolicyError      error

	DeletePolicyCalled    bool
	DeletePolicyPolicyARN string
	DeletePolicyError     error

	ListAttachedUserPoliciesCalled       bool
	ListAttachedUserPoliciesUserName     string
	ListAttachedUserPoliciesUserPolicies []string
	ListAttachedUserPoliciesError        error

	AttachUserPolicyCalled    bool
	AttachUserPolicyUserName  string
	AttachUserPolicyPolicyARN string
	AttachUserPolicyError     error

	DetachUserPolicyCalled    bool
	DetachUserPolicyUserName  string
	DetachUserPolicyPolicyARN string
	DetachUserPolicyError     error
}

func (f *FakeUser) Describe(userName string) (awsiam.UserDetails, error) {
	f.DescribeCalled = true
	f.DescribeUserName = userName

	return f.DescribeUserDetails, f.DescribeError
}

func (f *FakeUser) Create(userName string) (string, error) {
	f.CreateCalled = true
	f.CreateUserName = userName

	return f.CreateUserARN, f.CreateError
}

func (f *FakeUser) Delete(userName string) error {
	f.DeleteCalled = true
	f.DeleteUserName = userName

	return f.DeleteError
}

func (f *FakeUser) ListAccessKeys(userName string) ([]string, error) {
	f.ListAccessKeysCalled = true
	f.ListAccessKeysUserName = userName

	return f.ListAccessKeysAccessKeys, f.ListAccessKeysError
}

func (f *FakeUser) CreateAccessKey(userName string) (string, string, error) {
	f.CreateAccessKeyCalled = true
	f.CreateAccessKeyUserName = userName

	return f.CreateAccessKeyAccessKeyID, f.CreateAccessKeySecretAccessKey, f.CreateAccessKeyError
}

func (f *FakeUser) DeleteAccessKey(userName string, accessKeyID string) error {
	f.DeleteAccessKeyCalled = true
	f.DeleteAccessKeyUserName = userName
	f.DeleteAccessKeyAccessKeyID = accessKeyID

	return f.DeleteAccessKeyError
}

func (f *FakeUser) CreatePolicy(policyName string, effect string, action string, resource string) (string, error) {
	f.CreatePolicyCalled = true
	f.CreatePolicyPolicyName = policyName
	f.CreatePolicyEffect = effect
	f.CreatePolicyAction = action
	f.CreatePolicyResource = resource

	return f.CreatePolicyPolicyARN, f.CreatePolicyError
}

func (f *FakeUser) DeletePolicy(policyARN string) error {
	f.DeletePolicyCalled = true
	f.DeletePolicyPolicyARN = policyARN

	return f.DeletePolicyError
}

func (f *FakeUser) ListAttachedUserPolicies(userName string) ([]string, error) {
	f.ListAttachedUserPoliciesCalled = true
	f.ListAttachedUserPoliciesUserName = userName

	return f.ListAttachedUserPoliciesUserPolicies, f.ListAttachedUserPoliciesError
}

func (f *FakeUser) AttachUserPolicy(userName string, policyARN string) error {
	f.AttachUserPolicyCalled = true
	f.AttachUserPolicyUserName = userName
	f.AttachUserPolicyPolicyARN = policyARN

	return f.AttachUserPolicyError
}

func (f *FakeUser) DetachUserPolicy(userName string, policyARN string) error {
	f.DetachUserPolicyCalled = true
	f.DetachUserPolicyUserName = userName
	f.DetachUserPolicyPolicyARN = policyARN

	return f.DetachUserPolicyError
}
