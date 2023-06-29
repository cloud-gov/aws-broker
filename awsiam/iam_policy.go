package awsiam

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"text/template"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
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

type IamPolicyHandler struct {
	iamsvc iamiface.IAMAPI // *iam.IAM interface, doing this allows for test mocking
	logger lager.Logger
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

func NewIamPolicyHandler(region string, logger lager.Logger) *IamPolicyHandler {
	newsession := session.Must(session.NewSession())
	return &IamPolicyHandler{
		iamsvc: iam.New(newsession, aws.NewConfig().WithRegion(region)),
		logger: logger.Session("iam-policy"),
	}
}

func logAWSError(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		// Generic AWS error with Code, Message, and original error (if any)
		fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
		}
	} else {
		// This case should never be hit, the SDK should always return an
		// error which satisfies the awserr.Error interface.
		fmt.Println(err.Error())
	}
}

// create new assumable role with the trust policy
func (ip *IamPolicyHandler) CreateAssumeRole(policy string, rolename string) (*iam.Role, error) {
	role := &iam.Role{}
	roleInput := &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(policy),
		RoleName:                 aws.String(rolename),
	}
	resp, err := ip.iamsvc.CreateRole(roleInput)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == iam.ErrCodeEntityAlreadyExistsException {
				fmt.Println(iam.ErrCodeEntityAlreadyExistsException, awsErr.Error())
				fmt.Printf("role %s already exists, continuing\n", rolename)
				resp, innerErr := ip.iamsvc.GetRole(&iam.GetRoleInput{
					RoleName: aws.String(rolename),
				})
				if innerErr != nil {
					logAWSError(err)
					return nil, innerErr
				}
				return resp.Role, nil
			}
		}
		logAWSError(err)
		return role, err
	}

	return resp.Role, nil
}

func (ip *IamPolicyHandler) CreatePolicyFromTemplate(
	policyName,
	iamPath,
	policyTemplate string,
	resources []string,
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
		ip.logger.Error("aws-iam-error", err)
		return "", err
	}
	policy := bytes.Buffer{}
	err = tmpl.Execute(&policy, map[string]interface{}{
		"Resource":  resources[0],
		"Resources": resources,
	})
	if err != nil {
		ip.logger.Error("aws-iam-error", err)
		return "", err
	}

	createPolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policy.String()),
		Path:           stringOrNil(iamPath),
	}
	ip.logger.Debug("create-policy", lager.Data{"input": createPolicyInput})

	createPolicyOutput, err := ip.iamsvc.CreatePolicy(createPolicyInput)
	if err != nil {
		ip.logger.Error("aws-iam-error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return "", errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return "", err
	}
	ip.logger.Debug("create-policy", lager.Data{"output": createPolicyOutput})

	return aws.StringValue(createPolicyOutput.Policy.Arn), nil
}

// create a policy and attach to a user, return the policy ARN
// the does not validate the policy
func (ip *IamPolicyHandler) CreateUserPolicy(policy string, policyname string, username string) (string, error) {

	IamRolePolicyARN := ""

	rolePolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyname),
		PolicyDocument: aws.String(policy),
	}

	respPolicy, err := ip.iamsvc.CreatePolicy(rolePolicyInput)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == iam.ErrCodeEntityAlreadyExistsException {
				fmt.Println(iam.ErrCodeEntityAlreadyExistsException, awsErr.Error())
				fmt.Printf("policy name %s already exists, attempting to get policy ARN\n", policyname)
				resp, innerErr := ip.iamsvc.ListAttachedUserPolicies(&iam.ListAttachedUserPoliciesInput{
					UserName: aws.String(username),
				})
				if innerErr != nil {
					logAWSError(err)
					return "", innerErr
				}
				for _, policy := range resp.AttachedPolicies {
					if *policy.PolicyName == policyname {
						fmt.Printf("found policy ARN %s for policy %s\n", *policy.PolicyArn, policyname)
						return *policy.PolicyArn, nil
					}
				}
				return "", err
			}
		}
		logAWSError(err)
		// return if error
		return IamRolePolicyARN, err
	}

	fmt.Println(awsutil.Prettify(respPolicy))

	if respPolicy.Policy.Arn != nil {
		IamRolePolicyARN = *(respPolicy.Policy.Arn)
		userAttachPolicyInput := &iam.AttachUserPolicyInput{
			PolicyArn: aws.String(IamRolePolicyARN),
			UserName:  aws.String(username),
		}
		_, err := ip.iamsvc.AttachUserPolicy(userAttachPolicyInput)
		if err != nil {
			logAWSError(err)
			return IamRolePolicyARN, err
		}
	}
	return IamRolePolicyARN, nil
}

// create a new policy and attach to a specific role
// this does not validate the policy
func (ip *IamPolicyHandler) CreatePolicyAttachRole(policyname string, policy string, role iam.Role) (policyarn string, err error) {
	rolePolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyname), //(i.Domain + "-to-S3-RolePolicy"),
		PolicyDocument: aws.String(policy),
	}

	respPolicy, err := ip.iamsvc.CreatePolicy(rolePolicyInput)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == iam.ErrCodeEntityAlreadyExistsException {
				fmt.Println(iam.ErrCodeEntityAlreadyExistsException, awsErr.Error())
				fmt.Printf("policy name %s already exists, attempting to get policy ARN\n", policyname)
				resp, innerErr := ip.iamsvc.ListAttachedRolePolicies(&iam.ListAttachedRolePoliciesInput{
					RoleName: role.RoleName,
				})
				if innerErr != nil {
					logAWSError(err)
					return "", innerErr
				}
				for _, policy := range resp.AttachedPolicies {
					if *policy.PolicyName == policyname {
						fmt.Printf("found policy ARN %s for role %s\n", *policy.PolicyArn, *role.RoleName)
						return *policy.PolicyArn, nil
					}
				}
				return "", err
			}
		}
		logAWSError(err)
		return policyarn, err
	}
	if respPolicy.Policy.Arn != nil && role.RoleName != nil {
		policyarn = *(respPolicy.Policy.Arn)
		roleAttachPolicyInput := &iam.AttachRolePolicyInput{
			PolicyArn: aws.String(policyarn),
			RoleName:  aws.String(*(role.RoleName)),
		}

		respAttachPolicy, err := ip.iamsvc.AttachRolePolicy(roleAttachPolicyInput)
		if err != nil {
			logAWSError(err)
			return policyarn, err
		}
		fmt.Println(awsutil.Prettify(respAttachPolicy))
	}
	return policyarn, nil
}

// update a specific policy by adding new statements and updating the policyversion
// this does not validate the policy
func (ip IamPolicyHandler) UpdateExistingPolicy(policyARN string, policyStatements []PolicyStatementEntry) (*iam.PolicyVersion, error) {
	var policyDoc PolicyDocument
	var respPolVer *(iam.PolicyVersion)
	// get existing policy
	resPolicy, err := ip.iamsvc.GetPolicy(&iam.GetPolicyInput{
		PolicyArn: aws.String(policyARN),
	})
	if err != nil {
		logAWSError(err)
		fmt.Printf("UpdateExistingPolicy.GetPolicy with arn: %s failed\n", policyARN)
		return respPolVer, err
	}
	// get existing policy's current version number
	if resPolicy.Policy.DefaultVersionId != nil {
		policyVersionInput := &iam.GetPolicyVersionInput{
			PolicyArn: aws.String(policyARN),
			VersionId: aws.String(*(resPolicy.Policy.DefaultVersionId)),
		}
		resPolicyVersion, err := ip.iamsvc.GetPolicyVersion(policyVersionInput)
		if err != nil {
			logAWSError(err)
			fmt.Printf("UpdateExistingPolicy.GetPolicyVersion Failed with: %s failed\n", *(resPolicy.Policy.DefaultVersionId))
			return respPolVer, err
		}

		// convert policy document string into PolicyDocument
		if resPolicyVersion.PolicyVersion.Document != nil {
			err = policyDoc.FromString(*resPolicyVersion.PolicyVersion.Document)
			if err != nil {
				fmt.Printf("UpdateExistingPolicy.ConvertToPolicyDoc Failed with: %s failed\n", (*resPolicyVersion.PolicyVersion.Document))
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
			SetAsDefault:   aws.Bool(true),
		}

		err = ip.trimPolicyVersions(policyARN, 5)
		if err != nil {
			return respPolVer, err
		}

		resp, err := ip.iamsvc.CreatePolicyVersion(policyUpdatedVersion)
		if err != nil {
			logAWSError(err)
			fmt.Printf("UpdateExistingPolicy.CreatePolicyVersion Failed with: %v\n", policyUpdatedVersion)
			return respPolVer, err
		}
		if resp.PolicyVersion != nil {
			respPolVer = resp.PolicyVersion
		}
		fmt.Printf("UpdateExistingPolicy Success with: %v\n", respPolVer)
	}

	return respPolVer, nil
}

func (ip *IamPolicyHandler) DeletePolicy(policyARN string) error {
	deletePolicyInput := &iam.DeletePolicyInput{
		PolicyArn: aws.String(policyARN),
	}

	// list and remove all versions but default first
	ip.deleteNonDefaultPolicyVersions(policyARN)

	ip.logger.Debug("delete-policy", lager.Data{"input": deletePolicyInput})
	deletePolicyOutput, err := ip.iamsvc.DeletePolicy(deletePolicyInput)
	if err != nil {
		ip.logger.Error("delete-policy error", err)
		if awsErr, ok := err.(awserr.Error); ok {
			return errors.New(awsErr.Code() + ": " + awsErr.Message())
		}
		return err
	}
	ip.logger.Debug("delete-policy", lager.Data{"output": deletePolicyOutput})

	return nil
}

func (ip IamPolicyHandler) deleteNonDefaultPolicyVersions(policyARN string) error {
	input := &iam.ListPolicyVersionsInput{
		PolicyArn: &policyARN,
	}

	listPolicyVersionsOutput, err := ip.iamsvc.ListPolicyVersions(input)
	ip.logger.Debug("list-policy-versions", lager.Data{"listVersions": listPolicyVersionsOutput})
	if err != nil {
		ip.logger.Error("list-policy-versions error", err)
		return err
	}

	for _, version := range listPolicyVersionsOutput.Versions {
		if !(*version.IsDefaultVersion) {
			ip.logger.Debug("delete-policy deleting version", lager.Data{"version": version})
			_, err := ip.iamsvc.DeletePolicyVersion(&iam.DeletePolicyVersionInput{
				VersionId: version.VersionId,
				PolicyArn: aws.String(policyARN),
			})
			if err != nil {
				ip.logger.Error("aws-iam-error", err)
				if awsErr, ok := err.(awserr.Error); ok {
					return errors.New(awsErr.Code() + ": " + awsErr.Message())
				}
				return err
			}
		}
	}

	return nil
}

// we make sure we have space to create a new version by deleting the oldest.
func (ip IamPolicyHandler) trimPolicyVersions(policyARN string, maxVersions int) error {
	input := &iam.ListPolicyVersionsInput{
		PolicyArn: &policyARN,
	}

	resPolVers, err := ip.iamsvc.ListPolicyVersions(input)
	ip.logger.Debug("list-policy-versions", lager.Data{"listVersions": resPolVers})
	if err != nil {
		ip.logger.Error("list-policy-versions error", err)
		return err
	}

	// check if we have the max versions allowed then remove the earliest
	if len(resPolVers.Versions) >= maxVersions {
		sort.Slice(resPolVers.Versions, func(i, j int) bool {
			return *(resPolVers.Versions[i].VersionId) < *(resPolVers.Versions[j].VersionId)
		})
		for i := 0; i <= len(resPolVers.Versions)-(maxVersions+1); i++ {
			version := resPolVers.Versions[i]
			if !*(version.IsDefaultVersion) {
				input := &iam.DeletePolicyVersionInput{
					PolicyArn: &policyARN,
					VersionId: version.VersionId,
				}
				ip.logger.Debug("delete-policy deleting version", lager.Data{"version": version})
				_, err := ip.iamsvc.DeletePolicyVersion(input)
				if err != nil {
					ip.logger.Error("delete-policy-version error", err)
					return err
				}
			}
		}
	}
	return nil
}
