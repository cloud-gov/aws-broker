package awsiam

import (
	"context"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

type IAMUserClient struct {
	iamsvc IAMClientInterface
	logger lager.Logger
}

func NewIAMUserClient(
	iamsvc IAMClientInterface,
	logger lager.Logger,
) *IAMUserClient {
	return &IAMUserClient{
		iamsvc: iamsvc,
		logger: logger.Session("iam-user"),
	}
}

func (i *IAMUserClient) Describe(userName string) (UserDetails, error) {
	userDetails := UserDetails{
		UserName: userName,
	}

	getUserInput := &iam.GetUserInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("get-user", lager.Data{"input": getUserInput})

	getUserOutput, err := i.iamsvc.GetUser(context.TODO(), getUserInput)
	if err != nil {
		i.logger.Error("Describe: GetUser err", err)
		return userDetails, err
	}
	i.logger.Debug("get-user", lager.Data{"output": getUserOutput})

	userDetails.UserARN = *getUserOutput.User.Arn
	userDetails.UserID = *getUserOutput.User.UserId

	return userDetails, nil
}

func (i *IAMUserClient) Create(userName, iamPath string, iamTags []iamTypes.Tag) (string, error) {
	createUserInput := &iam.CreateUserInput{
		UserName: aws.String(userName),
		Path:     stringOrNil(iamPath),
		Tags:     iamTags,
	}
	i.logger.Debug("create-user", lager.Data{"input": createUserInput})

	createUserOutput, err := i.iamsvc.CreateUser(context.TODO(), createUserInput)
	if err != nil {
		i.logger.Error("Create: CreateUser err", err)
		return "", err
	}
	i.logger.Debug("create-user", lager.Data{"output": createUserOutput})

	return *createUserOutput.User.Arn, nil
}

func (i *IAMUserClient) Delete(userName string) error {
	deleteUserInput := &iam.DeleteUserInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("delete-user", lager.Data{"input": deleteUserInput})

	deleteUserOutput, err := i.iamsvc.DeleteUser(context.TODO(), deleteUserInput)
	if err != nil {
		i.logger.Error("Delete: DeleteUser err", err)
		return err
	}
	i.logger.Debug("delete-user", lager.Data{"output": deleteUserOutput})

	return nil
}

func (i *IAMUserClient) ListAccessKeys(userName string) ([]string, error) {
	var accessKeys []string

	listAccessKeysInput := &iam.ListAccessKeysInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("list-access-keys", lager.Data{"input": listAccessKeysInput})

	listAccessKeysOutput, err := i.iamsvc.ListAccessKeys(context.TODO(), listAccessKeysInput)
	if err != nil {
		i.logger.Error("ListAccessKeys: ListAccessKeys err", err)
		return accessKeys, err
	}
	i.logger.Debug("list-access-keys", lager.Data{"output": listAccessKeysOutput})

	for _, accessKey := range listAccessKeysOutput.AccessKeyMetadata {
		accessKeys = append(accessKeys, *accessKey.AccessKeyId)
	}

	return accessKeys, nil
}

func (i *IAMUserClient) CreateAccessKey(userName string) (string, string, error) {
	createAccessKeyInput := &iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("create-access-key", lager.Data{"input": createAccessKeyInput})

	createAccessKeyOutput, err := i.iamsvc.CreateAccessKey(context.TODO(), createAccessKeyInput)
	if err != nil {
		i.logger.Error("CreateAccessKey: CreateAccessKey err", err)
		return "", "", err
	}
	i.logger.Debug("create-access-key", lager.Data{"output": createAccessKeyOutput})

	return *createAccessKeyOutput.AccessKey.AccessKeyId, *createAccessKeyOutput.AccessKey.SecretAccessKey, nil
}

func (i *IAMUserClient) DeleteAccessKey(userName, accessKeyID string) error {
	deleteAccessKeyInput := &iam.DeleteAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: aws.String(accessKeyID),
	}
	i.logger.Debug("delete-access-key", lager.Data{"input": deleteAccessKeyInput})

	deleteAccessKeyOutput, err := i.iamsvc.DeleteAccessKey(context.TODO(), deleteAccessKeyInput)
	if err != nil {
		i.logger.Error("DeleteAccessKey: DeleteAccessKey err", err)
		return err
	}
	i.logger.Debug("delete-access-key", lager.Data{"output": deleteAccessKeyOutput})

	return nil
}

func (i *IAMUserClient) ListAttachedUserPolicies(userName, iamPath string) ([]string, error) {
	var userPolicies []string

	listAttachedUserPoliciesInput := &iam.ListAttachedUserPoliciesInput{
		UserName:   aws.String(userName),
		PathPrefix: stringOrNil(iamPath),
	}
	i.logger.Debug("list-attached-user-policies", lager.Data{"input": listAttachedUserPoliciesInput})

	listAttachedUserPoliciesOutput, err := i.iamsvc.ListAttachedUserPolicies(context.TODO(), listAttachedUserPoliciesInput)
	if err != nil {
		i.logger.Error("ListAttachedUserPolicies: ListAttachedUserPolicies err", err)
		return userPolicies, err
	}
	i.logger.Debug("list-attached-user-policies", lager.Data{"output": listAttachedUserPoliciesOutput})

	for _, userPolicy := range listAttachedUserPoliciesOutput.AttachedPolicies {
		userPolicies = append(userPolicies, *userPolicy.PolicyArn)
	}

	return userPolicies, nil
}

func (i *IAMUserClient) AttachUserPolicy(userName string, policyARN string) error {
	attachUserPolicyInput := &iam.AttachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(userName),
	}
	i.logger.Debug("attach-user-policy", lager.Data{"input": attachUserPolicyInput})

	attachUserPolicyOutput, err := i.iamsvc.AttachUserPolicy(context.TODO(), attachUserPolicyInput)
	if err != nil {
		i.logger.Error("AttachUserPolicy: AttachUserPolicy err", err)
		return err
	}
	i.logger.Debug("attach-user-policy", lager.Data{"output": attachUserPolicyOutput})

	return nil
}

func (i *IAMUserClient) DetachUserPolicy(userName string, policyARN string) error {
	detachUserPolicyInput := &iam.DetachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(userName),
	}
	i.logger.Debug("detach-user-policy", lager.Data{"input": detachUserPolicyInput})

	detachUserPolicyOutput, err := i.iamsvc.DetachUserPolicy(context.TODO(), detachUserPolicyInput)
	if err != nil {
		i.logger.Error("DetachUserPolicy: DetachUserPolicy err", err)
		return err
	}
	i.logger.Debug("detach-user-policy", lager.Data{"output": detachUserPolicyOutput})

	return nil
}

func stringOrNil(v string) *string {
	if v != "" {
		return &v
	}
	return nil
}
