package awsiam

import (
	"context"
	"log/slog"

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
	i.logger.Debug("create-user", "input", createUserInput)

	createUserOutput, err := i.iamsvc.CreateUser(context.TODO(), createUserInput)
	if err != nil {
		i.logger.Error("Create: CreateUser err", "err", err)
		return "", err
	}
	i.logger.Debug("create-user", "output", createUserOutput)

	return *createUserOutput.User.Arn, nil
}

func (i *IAMUserClient) Delete(userName string) error {
	deleteUserInput := &iam.DeleteUserInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("delete-user", "input", deleteUserInput)

	deleteUserOutput, err := i.iamsvc.DeleteUser(context.TODO(), deleteUserInput)
	if err != nil {
		i.logger.Error("Delete: DeleteUser err", "err", err)
		return err
	}
	i.logger.Debug("delete-user", "output", deleteUserOutput)

	return nil
}

func (i *IAMUserClient) CreateAccessKey(userName string) (string, string, error) {
	createAccessKeyInput := &iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("create-access-key", "input", createAccessKeyInput)

	createAccessKeyOutput, err := i.iamsvc.CreateAccessKey(context.TODO(), createAccessKeyInput)
	if err != nil {
		i.logger.Error("CreateAccessKey: CreateAccessKey err", "err", err)
		return "", "", err
	}
	i.logger.Debug("create-access-key", "output", createAccessKeyOutput)

	return *createAccessKeyOutput.AccessKey.AccessKeyId, *createAccessKeyOutput.AccessKey.SecretAccessKey, nil
}

func (i *IAMUserClient) DeleteAccessKey(userName, accessKeyID string) error {
	deleteAccessKeyInput := &iam.DeleteAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: aws.String(accessKeyID),
	}
	i.logger.Debug("delete-access-key", "input", deleteAccessKeyInput)

	deleteAccessKeyOutput, err := i.iamsvc.DeleteAccessKey(context.TODO(), deleteAccessKeyInput)
	if err != nil {
		i.logger.Error("DeleteAccessKey: DeleteAccessKey err", "err", err)
		return err
	}
	i.logger.Debug("delete-access-key", "output", deleteAccessKeyOutput)

	return nil
}

func (i *IAMUserClient) AttachUserPolicy(userName string, policyARN string) error {
	attachUserPolicyInput := &iam.AttachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(userName),
	}
	i.logger.Debug("attach-user-policy", "intput", attachUserPolicyInput)

	attachUserPolicyOutput, err := i.iamsvc.AttachUserPolicy(context.TODO(), attachUserPolicyInput)
	if err != nil {
		i.logger.Error("AttachUserPolicy: AttachUserPolicy err", "err", err)
		return err
	}
	i.logger.Debug("attach-user-policy", "output", attachUserPolicyOutput)

	return nil
}

func (i *IAMUserClient) DetachUserPolicy(userName string, policyARN string) error {
	detachUserPolicyInput := &iam.DetachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(userName),
	}
	i.logger.Debug("detach-user-policy", "input", detachUserPolicyInput)

	detachUserPolicyOutput, err := i.iamsvc.DetachUserPolicy(context.TODO(), detachUserPolicyInput)
	if err != nil {
		i.logger.Error("DetachUserPolicy: DetachUserPolicy err", "err", err)
		return err
	}
	i.logger.Debug("detach-user-policy", "output", detachUserPolicyOutput)

	return nil
}

func stringOrNil(v string) *string {
	if v != "" {
		return &v
	}
	return nil
}
