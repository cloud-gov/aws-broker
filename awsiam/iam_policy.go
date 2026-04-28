package awsiam

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
)

type PolicyDocument struct {
	Version   string
	Statement []PolicyStatementEntry
}

type PolicyStatementEntry struct {
	Effect   string
	Action   []string
	Resource []string
}

func (ps *PolicyStatementEntry) ToString() (string, error) {
	retbytes, err := json.Marshal(ps)
	rval := string(retbytes)
	return rval, err
}

type IAMClientInterface interface {
	AttachRolePolicy(ctx context.Context, params *iam.AttachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error)
	AttachUserPolicy(ctx context.Context, params *iam.AttachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.AttachUserPolicyOutput, error)
	CreateAccessKey(ctx context.Context, params *iam.CreateAccessKeyInput, optFns ...func(*iam.Options)) (*iam.CreateAccessKeyOutput, error)
	CreatePolicy(ctx context.Context, params *iam.CreatePolicyInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyOutput, error)
	CreatePolicyVersion(ctx context.Context, params *iam.CreatePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error)
	CreateRole(ctx context.Context, params *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error)
	CreateUser(ctx context.Context, params *iam.CreateUserInput, optFns ...func(*iam.Options)) (*iam.CreateUserOutput, error)
	DeleteAccessKey(ctx context.Context, params *iam.DeleteAccessKeyInput, optFns ...func(*iam.Options)) (*iam.DeleteAccessKeyOutput, error)
	DeletePolicy(ctx context.Context, params *iam.DeletePolicyInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyOutput, error)
	DeleteRole(ctx context.Context, params *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error)
	DeletePolicyVersion(ctx context.Context, params *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error)
	DeleteUser(ctx context.Context, params *iam.DeleteUserInput, optFns ...func(*iam.Options)) (*iam.DeleteUserOutput, error)
	DetachRolePolicy(ctx context.Context, params *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error)
	DetachUserPolicy(ctx context.Context, params *iam.DetachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error)
	GetPolicy(ctx context.Context, params *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error)
	GetPolicyVersion(ctx context.Context, params *iam.GetPolicyVersionInput, optFns ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error)
	GetRole(ctx context.Context, params *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error)
	GetUser(ctx context.Context, params *iam.GetUserInput, optFns ...func(*iam.Options)) (*iam.GetUserOutput, error)
	ListAttachedRolePolicies(ctx context.Context, params *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error)
	ListAttachedUserPolicies(ctx context.Context, params *iam.ListAttachedUserPoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedUserPoliciesOutput, error)
	ListPolicyVersions(ctx context.Context, params *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error)
}

func (pd *PolicyDocument) ToString() (string, error) {

	retbytes, err := json.Marshal(pd)
	rval := string(retbytes)
	return rval, err
}

func (pd *PolicyDocument) FromString(docstring string) error {
	decodedstr, err := url.QueryUnescape(docstring)
	if err != nil {
		return fmt.Errorf("UrlDecoding failed in PolicyDoc.FromString")
	}
	err = json.Unmarshal([]byte(decodedstr), &pd)
	return err
}

// adds any policy statement entries that dont already exist in the policy doc
// using string comparison
func (pd *PolicyDocument) AddNewStatements(newStatements []PolicyStatementEntry) bool {
	var modified bool = false
	searchkeys := map[string]string{}
	for _, v := range pd.Statement {
		key, _ := v.ToString()
		searchkeys[key] = ""
	}
	for _, newpol := range newStatements {
		key, _ := newpol.ToString()
		if _, ok := searchkeys[key]; !ok {
			pd.Statement = append(pd.Statement, newpol)
			modified = true
		}
	}
	return modified
}

// create new assumable role with the trust policy
func CreateAssumeRole(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policy string,
	rolename string,
	iamTags []types.Tag,
) (*types.Role, error) {
	role := &types.Role{}
	roleInput := &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(policy),
		RoleName:                 aws.String(rolename),
		Tags:                     iamTags,
	}
	resp, err := iamClient.CreateRole(ctx, roleInput)
	if err != nil {
		var alreadyExistsException *types.EntityAlreadyExistsException
		if errors.As(err, &alreadyExistsException) {
			logger.Debug(fmt.Sprintf("role %s already exists, continuing\n", rolename))

			resp, innerErr := iamClient.GetRole(ctx, &iam.GetRoleInput{
				RoleName: aws.String(rolename),
			})
			if innerErr != nil {
				logger.Error("CreateAssumeRole: GetRole error", "err", err)
				return nil, innerErr
			}
			return resp.Role, nil
		}

		logger.Error("CreateAssumeRole: CreateRole error", "err", err)
		return role, err
	}

	return resp.Role, nil
}

func CreatePolicyFromTemplate(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policyName string,
	iamPath string,
	policyTemplate string,
	resources []string,
	iamTags []types.Tag,
) (string, error) {
	tmpl, err := template.New("policy").Funcs(template.FuncMap{
		"resources": func(suffix string) string {
			resourcePaths := make([]string, len(resources))
			for idx, resource := range resources {
				resourcePaths[idx] = resource + suffix
			}
			marshaled, err := json.Marshal(resourcePaths)
			if err != nil {
				panic(err)
			}
			return string(marshaled)
		},
	}).Parse(policyTemplate)
	if err != nil {
		logger.Error("CreatePolicyFromTemplate: template.Parse error", "err", err)
		return "", err
	}
	policy := bytes.Buffer{}
	err = tmpl.Execute(&policy, map[string]interface{}{
		"Resource":  resources[0],
		"Resources": resources,
	})
	if err != nil {
		logger.Error("CreatePolicyFromTemplate: tmpl.Execute error", "err", err)
		return "", err
	}

	createPolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policy.String()),
		Path:           stringOrNil(iamPath),
		Tags:           iamTags,
	}
	logger.Debug("create-policy", "input", createPolicyInput)

	createPolicyOutput, err := iamClient.CreatePolicy(ctx, createPolicyInput)
	if err != nil {
		logger.Error("CreatePolicyFromTemplate: CreatePolicy error", "err", err)
		return "", err
	}
	logger.Debug("create-policy", "output", createPolicyOutput)

	return *createPolicyOutput.Policy.Arn, nil
}

// create a policy and attach to a user, return the policy ARN
// the does not validate the policy
func CreateUserPolicy(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policy string,
	policyname string,
	username string,
	iamTags []types.Tag,
) (string, error) {

	IamRolePolicyARN := ""

	rolePolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyname),
		PolicyDocument: aws.String(policy),
		Tags:           iamTags,
	}

	respPolicy, err := iamClient.CreatePolicy(ctx, rolePolicyInput)
	if err != nil {
		var alreadyExistsException *types.EntityAlreadyExistsException
		if errors.As(err, &alreadyExistsException) {
			logger.Debug(fmt.Sprintf("policy name %s already exists, attempting to get policy ARN\n", policyname))

			resp, innerErr := iamClient.ListAttachedUserPolicies(ctx, &iam.ListAttachedUserPoliciesInput{
				UserName: aws.String(username),
			})

			if innerErr != nil {
				logger.Error("CreateUserPolicy: ListAttachedUserPolicies error", "err", innerErr)
				return "", innerErr
			}

			for _, policy := range resp.AttachedPolicies {
				if *policy.PolicyName == policyname {
					logger.Debug(fmt.Sprintf("found policy ARN %s for policy %s\n", *policy.PolicyArn, policyname))
					return *policy.PolicyArn, nil
				}
			}
			return "", err
		}

		logger.Error("CreateUserPolicy: CreatePolicy error", "err", err)

		// return if error
		return IamRolePolicyARN, err
	}

	if respPolicy.Policy.Arn != nil {
		IamRolePolicyARN = *(respPolicy.Policy.Arn)
		userAttachPolicyInput := &iam.AttachUserPolicyInput{
			PolicyArn: aws.String(IamRolePolicyARN),
			UserName:  aws.String(username),
		}
		_, err := iamClient.AttachUserPolicy(ctx, userAttachPolicyInput)
		if err != nil {
			logger.Error("CreateUserPolicy: AttachUserPolicy error", "err", err)
			return IamRolePolicyARN, err
		}
	}
	return IamRolePolicyARN, nil
}

// create a new policy and attach to a specific role
// this does not validate the policy
func CreatePolicyAttachRole(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policyname string,
	policy string,
	role types.Role,
	iamTags []types.Tag,
) (policyarn string, err error) {
	rolePolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyname), //(i.Domain + "-to-S3-RolePolicy"),
		PolicyDocument: aws.String(policy),
		Tags:           iamTags,
	}

	respPolicy, err := iamClient.CreatePolicy(ctx, rolePolicyInput)
	if err != nil {
		logger.Error("CreatePolicyAttachRole: CreatePolicy error", "err", err)

		var alreadyExistsException *types.EntityAlreadyExistsException
		if errors.As(err, &alreadyExistsException) {
			logger.Debug(fmt.Sprintf("policy name %s already exists, attempting to get policy ARN\n", policyname))
			resp, innerErr := iamClient.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{
				RoleName: role.RoleName,
			})
			if innerErr != nil {
				logger.Error("CreatePolicyAttachRole: ListAttachedRolePolicies error", "err", err)
				return "", innerErr
			}
			for _, policy := range resp.AttachedPolicies {
				if *policy.PolicyName == policyname {
					logger.Debug(fmt.Sprintf("found policy ARN %s for role %s\n", *policy.PolicyArn, *role.RoleName))
					return *policy.PolicyArn, nil
				}
			}
			return "", err
		}

		return policyarn, err
	}
	if respPolicy.Policy.Arn != nil && role.RoleName != nil {
		policyarn = *(respPolicy.Policy.Arn)
		roleAttachPolicyInput := &iam.AttachRolePolicyInput{
			PolicyArn: aws.String(policyarn),
			RoleName:  aws.String(*(role.RoleName)),
		}

		_, err := iamClient.AttachRolePolicy(ctx, roleAttachPolicyInput)
		if err != nil {
			logger.Error("CreatePolicyAttachRole: AttachRolePolicy error", "err", err)
			return policyarn, err
		}
	}
	return policyarn, nil
}

// update a specific policy by adding new statements and updating the policyversion
// this does not validate the policy
func UpdateExistingPolicy(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policyARN string,
	policyStatements []PolicyStatementEntry,
) (*types.PolicyVersion, error) {
	var policyDoc PolicyDocument
	var respPolVer *(types.PolicyVersion)
	// get existing policy
	resPolicy, err := iamClient.GetPolicy(ctx, &iam.GetPolicyInput{
		PolicyArn: aws.String(policyARN),
	})
	if err != nil {
		logger.Error("UpdateExistingPolicy: GetPolicy error", "err", err)
		logger.Debug(fmt.Sprintf("UpdateExistingPolicy.GetPolicy with arn: %s failed\n", policyARN))
		return respPolVer, err
	}
	// get existing policy's current version number
	if resPolicy.Policy.DefaultVersionId != nil {
		policyVersionInput := &iam.GetPolicyVersionInput{
			PolicyArn: aws.String(policyARN),
			VersionId: aws.String(*(resPolicy.Policy.DefaultVersionId)),
		}
		resPolicyVersion, err := iamClient.GetPolicyVersion(ctx, policyVersionInput)
		if err != nil {
			logger.Error("UpdateExistingPolicy: GetPolicyVersion error", "err", err)
			logger.Debug(fmt.Sprintf("UpdateExistingPolicy.GetPolicyVersion Failed with: %s failed\n", *(resPolicy.Policy.DefaultVersionId)))
			return respPolVer, err
		}

		// convert policy document string into PolicyDocument
		if resPolicyVersion.PolicyVersion.Document != nil {
			err = policyDoc.FromString(*resPolicyVersion.PolicyVersion.Document)
			if err != nil {
				logger.Debug(fmt.Sprintf("UpdateExistingPolicy.ConvertToPolicyDoc Failed with: %s failed\n", (*resPolicyVersion.PolicyVersion.Document)))
				return respPolVer, err
			}
		}
	}

	// now try to add any new statements entries to PolicyDoc
	// if we succeed then create new policy version
	if policyDoc.AddNewStatements(policyStatements) {

		// convert PolicyDoc to string and create new policyversion to update policy
		docstring, err := policyDoc.ToString()
		if err != nil {
			return respPolVer, err
		}
		policyUpdatedVersion := &iam.CreatePolicyVersionInput{
			PolicyArn:      aws.String(policyARN),
			PolicyDocument: aws.String(docstring),
			SetAsDefault:   true,
		}

		err = trimPolicyVersions(ctx, iamClient, logger, policyARN, 5)
		if err != nil {
			return respPolVer, err
		}

		resp, err := iamClient.CreatePolicyVersion(ctx, policyUpdatedVersion)
		if err != nil {
			logger.Error("UpdateExistingPolicy: CreatePolicyVersion error", "err", err)
			logger.Debug(fmt.Sprintf("UpdateExistingPolicy.CreatePolicyVersion Failed with: %v\n", policyUpdatedVersion))
			return respPolVer, err
		}
		if resp.PolicyVersion != nil {
			respPolVer = resp.PolicyVersion
		}
		logger.Debug(fmt.Sprintf("UpdateExistingPolicy Success with: %v\n", respPolVer))
	}

	return respPolVer, nil
}

func DeletePolicy(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policyARN string,
) error {
	deletePolicyInput := &iam.DeletePolicyInput{
		PolicyArn: aws.String(policyARN),
	}

	// list and remove all versions but default first
	deleteNonDefaultPolicyVersions(ctx, iamClient, logger, policyARN)

	logger.Debug("delete-policy", "input", deletePolicyInput)
	deletePolicyOutput, err := iamClient.DeletePolicy(ctx, deletePolicyInput)
	if err != nil {
		logger.Error("DeletePolicy: DeletePolicy error", "err", err)
		return err
	}
	logger.Debug("delete-policy", "output", deletePolicyOutput)

	return nil
}

func deleteNonDefaultPolicyVersions(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policyARN string,
) error {
	input := &iam.ListPolicyVersionsInput{
		PolicyArn: &policyARN,
	}

	listPolicyVersionsOutput, err := iamClient.ListPolicyVersions(ctx, input)
	logger.Debug("list-policy-versions", "listVersions", listPolicyVersionsOutput)
	if err != nil {
		logger.Error("deleteNonDefaultPolicyVersions: ListPolicyVersions error", "err", err)
		return err
	}

	for _, version := range listPolicyVersionsOutput.Versions {
		if !version.IsDefaultVersion {
			logger.Debug("delete-policy deleting version", "version", version)
			_, err := iamClient.DeletePolicyVersion(ctx, &iam.DeletePolicyVersionInput{
				VersionId: version.VersionId,
				PolicyArn: aws.String(policyARN),
			})
			if err != nil {
				logger.Error("deleteNonDefaultPolicyVersions: DeletePolicyVersion error", "err", err)
				return err
			}
		}
	}

	return nil
}

// we make sure we have space to create a new version by deleting the oldest.
func trimPolicyVersions(
	ctx context.Context,
	iamClient IAMClientInterface,
	logger *slog.Logger,
	policyARN string,
	maxVersions int,
) error {
	input := &iam.ListPolicyVersionsInput{
		PolicyArn: &policyARN,
	}

	resPolVers, err := iamClient.ListPolicyVersions(ctx, input)
	logger.Debug("list-policy-versions", "listVersions", resPolVers)
	if err != nil {
		logger.Error("trimPolicyVersions: ListPolicyVersions error", "err", err)
		return err
	}

	// check if we have the max versions allowed then remove the earliest
	if len(resPolVers.Versions) >= maxVersions {
		sort.Slice(resPolVers.Versions, func(i, j int) bool {
			return *(resPolVers.Versions[i].VersionId) < *(resPolVers.Versions[j].VersionId)
		})
		for i := 0; i <= len(resPolVers.Versions)-(maxVersions+1); i++ {
			version := resPolVers.Versions[i]
			if !version.IsDefaultVersion {
				input := &iam.DeletePolicyVersionInput{
					PolicyArn: &policyARN,
					VersionId: version.VersionId,
				}
				logger.Debug("delete-policy deleting version", "version", version)
				_, err := iamClient.DeletePolicyVersion(ctx, input)
				if err != nil {
					logger.Error("trimPolicyVersions: DeletePolicyVersion error", "err", err)
					return err
				}
			}
		}
	}
	return nil
}
