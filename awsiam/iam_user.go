package awsiam

import (
	"bytes"
	"encoding/json"
	"errors"
	"text/template"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
)

type IAMUser struct {
	iamsvc *iam.IAM
	logger lager.Logger
}

func NewIAMUser(
	iamsvc *iam.IAM,
	logger lager.Logger,
) *IAMUser {
	return &IAMUser{
		iamsvc: iamsvc,
		logger: logger.Session("iam-user"),
	}
}

func (i *IAMUser) Describe(userName string) (UserDetails, error) {
	userDetails := UserDetails{
		UserName: userName,
	}

	getUserInput := &iam.GetUserInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("get-user", lager.Data{"input": getUserInput})

	getUserOutput, err := i.iamsvc.GetUser(getUserInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return userDetails, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return userDetails, err
	}
	i.logger.Debug("get-user", lager.Data{"output": getUserOutput})

	userDetails.UserARN = aws.StringValue(getUserOutput.User.Arn)
	userDetails.UserID = aws.StringValue(getUserOutput.User.UserId)

	return userDetails, nil
}

func (i *IAMUser) Create(userName, iamPath string) (string, error) {
	createUserInput := &iam.CreateUserInput{
		UserName: aws.String(userName),
		Path:     stringOrNil(iamPath),
	}
	i.logger.Debug("create-user", lager.Data{"input": createUserInput})

	createUserOutput, err := i.iamsvc.CreateUser(createUserInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return "", errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return "", err
	}
	i.logger.Debug("create-user", lager.Data{"output": createUserOutput})

	return aws.StringValue(createUserOutput.User.Arn), nil
}

func (i *IAMUser) Delete(userName string) error {
	deleteUserInput := &iam.DeleteUserInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("delete-user", lager.Data{"input": deleteUserInput})

	deleteUserOutput, err := i.iamsvc.DeleteUser(deleteUserInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	i.logger.Debug("delete-user", lager.Data{"output": deleteUserOutput})

	return nil
}

func (i *IAMUser) ListAccessKeys(userName string) ([]string, error) {
	var accessKeys []string

	listAccessKeysInput := &iam.ListAccessKeysInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("list-access-keys", lager.Data{"input": listAccessKeysInput})

	listAccessKeysOutput, err := i.iamsvc.ListAccessKeys(listAccessKeysInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return accessKeys, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return accessKeys, err
	}
	i.logger.Debug("list-access-keys", lager.Data{"output": listAccessKeysOutput})

	for _, accessKey := range listAccessKeysOutput.AccessKeyMetadata {
		accessKeys = append(accessKeys, aws.StringValue(accessKey.AccessKeyId))
	}

	return accessKeys, nil
}

func (i *IAMUser) CreateAccessKey(userName string) (string, string, error) {
	createAccessKeyInput := &iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	}
	i.logger.Debug("create-access-key", lager.Data{"input": createAccessKeyInput})

	createAccessKeyOutput, err := i.iamsvc.CreateAccessKey(createAccessKeyInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return "", "", errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return "", "", err
	}
	i.logger.Debug("create-access-key", lager.Data{"output": createAccessKeyOutput})

	return aws.StringValue(createAccessKeyOutput.AccessKey.AccessKeyId), aws.StringValue(createAccessKeyOutput.AccessKey.SecretAccessKey), nil
}

func (i *IAMUser) DeleteAccessKey(userName, accessKeyID string) error {
	deleteAccessKeyInput := &iam.DeleteAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: aws.String(accessKeyID),
	}
	i.logger.Debug("delete-access-key", lager.Data{"input": deleteAccessKeyInput})

	deleteAccessKeyOutput, err := i.iamsvc.DeleteAccessKey(deleteAccessKeyInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	i.logger.Debug("delete-access-key", lager.Data{"output": deleteAccessKeyOutput})

	return nil
}

func (i *IAMUser) CreatePolicy(policyName, iamPath, policyTemplate string, resources []string) (string, error) {
	tmpl, err := template.New("policy").Funcs(template.FuncMap{
		"resources": func(suffix string) string {
			resourcePaths := make([]string, len(resources))
			for idx, resource := range resources {
				resourcePaths[idx] = resource + suffix
			}
			marshaled, _ := json.Marshal(resourcePaths)
			return string(marshaled)
		},
	}).Parse(policyTemplate)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		return "", err
	}
	policy := bytes.Buffer{}
	err = tmpl.Execute(&policy, map[string]interface{}{
		"Resource":  resources[0],
		"Resources": resources,
	})
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		return "", err
	}

	createPolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policy.String()),
		Path:           stringOrNil(iamPath),
	}
	i.logger.Debug("create-policy", lager.Data{"input": createPolicyInput})

	createPolicyOutput, err := i.iamsvc.CreatePolicy(createPolicyInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return "", errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return "", err
	}
	i.logger.Debug("create-policy", lager.Data{"output": createPolicyOutput})

	return aws.StringValue(createPolicyOutput.Policy.Arn), nil
}

func (i *IAMUser) DeletePolicy(policyARN string) error {
	deletePolicyInput := &iam.DeletePolicyInput{
		PolicyArn: aws.String(policyARN),
	}
	i.logger.Debug("delete-policy", lager.Data{"input": deletePolicyInput})

	deletePolicyOutput, err := i.iamsvc.DeletePolicy(deletePolicyInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	i.logger.Debug("delete-policy", lager.Data{"output": deletePolicyOutput})

	return nil
}

func (i *IAMUser) ListAttachedUserPolicies(userName, iamPath string) ([]string, error) {
	var userPolicies []string

	listAttachedUserPoliciesInput := &iam.ListAttachedUserPoliciesInput{
		UserName:   aws.String(userName),
		PathPrefix: stringOrNil(iamPath),
	}
	i.logger.Debug("list-attached-user-policies", lager.Data{"input": listAttachedUserPoliciesInput})

	listAttachedUserPoliciesOutput, err := i.iamsvc.ListAttachedUserPolicies(listAttachedUserPoliciesInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return userPolicies, errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return userPolicies, err
	}
	i.logger.Debug("list-attached-user-policies", lager.Data{"output": listAttachedUserPoliciesOutput})

	for _, userPolicy := range listAttachedUserPoliciesOutput.AttachedPolicies {
		userPolicies = append(userPolicies, aws.StringValue(userPolicy.PolicyArn))
	}

	return userPolicies, nil
}

func (i *IAMUser) AttachUserPolicy(userName string, policyARN string) error {
	attachUserPolicyInput := &iam.AttachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(userName),
	}
	i.logger.Debug("attach-user-policy", lager.Data{"input": attachUserPolicyInput})

	attachUserPolicyOutput, err := i.iamsvc.AttachUserPolicy(attachUserPolicyInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	i.logger.Debug("attach-user-policy", lager.Data{"output": attachUserPolicyOutput})

	return nil
}

func (i *IAMUser) DetachUserPolicy(userName string, policyARN string) error {
	detachUserPolicyInput := &iam.DetachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(userName),
	}
	i.logger.Debug("detach-user-policy", lager.Data{"input": detachUserPolicyInput})

	detachUserPolicyOutput, err := i.iamsvc.DetachUserPolicy(detachUserPolicyInput)
	if err != nil {
		i.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
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
