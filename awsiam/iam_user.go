package awsiam

import (
	"context"
	"log/slog"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

type IAMUserClient struct {
	iamsvc IAMClientInterface
	logger *slog.Logger
}

func NewIAMUserClient(
	iamsvc IAMClientInterface,
	logger *slog.Logger,
) *IAMUserClient {
	return &IAMUserClient{
		iamsvc: iamsvc,
		logger: logger,
	}
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
