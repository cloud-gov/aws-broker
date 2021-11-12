package iampolicy

import (
	"encoding/json"
	"fmt"

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

type IamPolicyHandler struct {
	iamsvc iamiface.IAMAPI // *iam.IAM interface, doing this allows for test mocking
}

func (pd *PolicyDocument) ToString() (string, error) {
	retbytes, err := json.Marshal(pd)
	rval := string(retbytes)
	return rval, err
}

func (pd *PolicyDocument) FromString(docstring string) error {
	err := json.Unmarshal([]byte(docstring), &pd)
	return err
}

func NewIamPolicyHandler(region string) *IamPolicyHandler {
	ip := IamPolicyHandler{}
	newsession := session.Must(session.NewSession())
	ip.iamsvc = iam.New(newsession, aws.NewConfig().WithRegion(region))
	return &ip
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
		return role, err
	}

	return resp.Role, nil

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
		// return if error
		return IamRolePolicyARN, err
	}

	fmt.Println(awsutil.StringValue(respPolicy))

	if respPolicy.Policy.Arn != nil {
		IamRolePolicyARN = *(respPolicy.Policy.Arn)
		userAttachPolicyInput := &iam.AttachUserPolicyInput{
			PolicyArn: aws.String(*(respPolicy.Policy.Arn)),
			UserName:  aws.String(username),
		}
		_, err := ip.iamsvc.AttachUserPolicy(userAttachPolicyInput)
		if err != nil {
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
		return policyarn, err
	}
	if respPolicy.Policy.Arn != nil && role.RoleName != nil {
		policyarn = *(respPolicy.Policy.Arn)
		roleAttachPolicyInput := &iam.AttachRolePolicyInput{
			PolicyArn: aws.String(*(respPolicy.Policy.Arn)),
			RoleName:  aws.String(*(role.RoleName)),
		}

		respAttachPolicy, err := ip.iamsvc.AttachRolePolicy(roleAttachPolicyInput)
		if err != nil {
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
			return policyarn, err
		}
		fmt.Println(awsutil.StringValue(respAttachPolicy))
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
			return respPolVer, err
		}

		// convert policy document string into PolicyDocument
		if resPolicyVersion.PolicyVersion.Document != nil {
			err = policyDoc.FromString(*resPolicyVersion.PolicyVersion.Document)
			if err != nil {
				return respPolVer, err
			}
		}
	}
	// now add new statement entries to PolicyDoc
	policyDoc.Statement = append(policyDoc.Statement, policyStatements...)
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
	resp, err := ip.iamsvc.CreatePolicyVersion(policyUpdatedVersion)
	if err != nil {
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
		return respPolVer, err
	}
	if resp.PolicyVersion != nil {
		respPolVer = resp.PolicyVersion
	}
	return respPolVer, nil
}
