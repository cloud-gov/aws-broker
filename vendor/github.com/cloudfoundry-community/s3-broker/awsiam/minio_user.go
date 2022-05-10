package awsiam

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"text/template"

	"code.cloudfoundry.org/lager"
	awsarn "github.com/aws/aws-sdk-go/aws/arn"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
	"github.com/pkg/errors"
	"github.com/sethvargo/go-password/password"
)

type MinioUser struct {
	madmClnt *madmin.AdminClient
	logger   lager.Logger
	// preserved for subsequent connections (generating user-centric access keys)
	endpoint        string
	accessKey       string
	secretKey       string
	secure          bool
	customTransport http.RoundTripper
}

func NewMinioUser(
	logger lager.Logger,
	endpoint string,
	accessKey string,
	secretKey string,
	secure bool,
	customTransport http.RoundTripper,
) *MinioUser {
	madmClnt, err := newMinioAdminClient(endpoint, accessKey, secretKey, secure, customTransport)
	if err != nil {
		logger.Fatal("unable to create MinIO admin client", err)
	}

	return &MinioUser{
		madmClnt:        madmClnt,
		logger:          logger.Session("minio-user"),
		endpoint:        endpoint,
		accessKey:       accessKey,
		secretKey:       secretKey,
		secure:          secure,
		customTransport: customTransport,
	}
}

func newMinioAdminClient(endpoint, accessKey, secretKey string, secure bool, customTransport http.RoundTripper) (*madmin.AdminClient, error) {
	madmClnt, err := madmin.New(endpoint, accessKey, secretKey, secure)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to MinIO: %s", err)
	}
	madmClnt.SetCustomTransport(customTransport)
	return madmClnt, nil
}

func (i *MinioUser) Describe(userName string) (UserDetails, error) {
	// We simply echo data back; note that presence of the user is tested...
	userDetails := UserDetails{
		UserName: userName,
		UserARN:  toUserARN(userName),
		UserID:   userName,
	}

	i.logger.Debug("get-user", lager.Data{"input": userName})

	_, err := i.madmClnt.GetUserInfo(context.Background(), userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return userDetails, err
	}

	i.logger.Debug("get-user", lager.Data{"output": userDetails})

	return userDetails, nil
}

func (i *MinioUser) Create(userName, iamPath string) (string, error) {
	i.logger.Debug("create-user", lager.Data{"userName": userName, "iamPath": iamPath})

	secretKey, err := password.Generate(64, 10, 0, false, true)
	if err != nil {
		i.logger.Error("password-generator", err)
		return "", err
	}

	err = i.madmClnt.AddUser(context.Background(), userName, secretKey)
	if err != nil {
		i.logger.Error("minio-error", err)
		return "", err
	}
	i.logger.Debug("create-user", lager.Data{"output": userName})

	return toUserARN(userName), nil
}

func (i *MinioUser) Delete(userName string) error {
	i.logger.Debug("delete-user", lager.Data{"userName": userName})

	err := i.madmClnt.RemoveUser(context.Background(), userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return err
	}

	i.logger.Debug("delete-user", lager.Data{"output": userName})

	return nil
}

func (i *MinioUser) createUserClient(userName string) (*madmin.AdminClient, error) {
	// UserInfo does not appear to respond with actual SecretKey
	// ... so we just set it again so we can auth to it and create a key!
	secretKey, err := password.Generate(64, 10, 0, false, true)
	if err != nil {
		i.logger.Error("createUserClient/password-generator", err)
		return nil, err
	}

	err = i.madmClnt.SetUser(context.Background(), userName, secretKey, madmin.AccountEnabled)
	if err != nil {
		i.logger.Error("createUserClient/minio-error", err)
		return nil, err
	}

	// Connect as alternate user
	return newMinioAdminClient(i.endpoint, userName, secretKey, i.secure, i.customTransport)
}

func (i *MinioUser) ListAccessKeys(userName string) ([]string, error) {
	i.logger.Debug("list-access-keys", lager.Data{"input": userName})

	var accessKeys []string

	// Connect as alternate user
	userClient, err := i.createUserClient(userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return accessKeys, err
	}

	accounts, err := userClient.ListServiceAccounts(context.Background())
	if err != nil {
		i.logger.Error("minio-error", err)
		return accessKeys, err
	}

	i.logger.Debug("list-access-keys", lager.Data{"output": accounts.Accounts})

	return accounts.Accounts, nil
}

func (i *MinioUser) CreateAccessKey(userName string) (string, string, error) {
	i.logger.Debug("create-access-key", lager.Data{"input": userName})

	info, err := i.madmClnt.GetUserInfo(context.Background(), userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return "", "", err
	}

	var policy *iampolicy.Policy
	if info.PolicyName != "" {
		policy, err = i.madmClnt.InfoCannedPolicy(context.Background(), info.PolicyName)
		if err != nil {
			i.logger.Error("minio-error", err)
			return "", "", err
		}
	}

	// Connect as alternate user
	userClient, err := i.createUserClient(userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return "", "", err
	}

	creds, err := userClient.AddServiceAccount(context.Background(), policy)
	if err != nil {
		i.logger.Error("minio-error", err)
		return "", "", err
	}

	i.logger.Debug("create-access-key", lager.Data{"output": creds.AccessKey})

	return creds.AccessKey, creds.SecretKey, nil
}

func (i *MinioUser) DeleteAccessKey(userName, accessKeyID string) error {
	i.logger.Debug("delete-access-key", lager.Data{"input": userName})

	// Connect as alternate user
	userClient, err := i.createUserClient(userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return err
	}

	err = userClient.DeleteServiceAccount(context.Background(), accessKeyID)
	if err != nil {
		i.logger.Error("minio-error", err)
		return err
	}

	i.logger.Debug("delete-access-key", lager.Data{"output": "N/A"})

	return nil
}

func (i *MinioUser) CreatePolicy(policyName, iamPath, policyTemplate string, resources []string) (string, error) {
	i.logger.Debug("create-policy", lager.Data{"policyName": policyName, "iamPath": iamPath, "policyTemplate": policyTemplate, "resources": resources})

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
		i.logger.Error("parse-error", err)
		return "", err
	}
	policy := bytes.Buffer{}
	err = tmpl.Execute(&policy, map[string]interface{}{
		"Resource":  resources[0],
		"Resources": resources,
	})
	if err != nil {
		i.logger.Error("template-error", err)
		return "", err
	}

	policyInput := iampolicy.Policy{}
	err = json.Unmarshal(policy.Bytes(), &policyInput)
	if err != nil {
		i.logger.Error("unmarshal-error", err)
		return "", err
	}
	i.logger.Debug("create-policy", lager.Data{"input": policyInput})

	err = i.madmClnt.AddCannedPolicy(context.Background(), policyName, &policyInput)
	if err != nil {
		i.logger.Error("minio-add-policy-error", err)
		return "", err
	}

	i.logger.Debug("create-policy", lager.Data{"output": "created"})

	return toPolicyARN(policyName), nil
}

func (i *MinioUser) DeletePolicy(policyARN string) error {
	i.logger.Debug("delete-policy", lager.Data{"input": policyARN})

	policy, err := fromPolicyARN(policyARN)
	if err != nil {
		i.logger.Error("minio-arn-error", err)
		return err
	}

	err = i.madmClnt.RemoveCannedPolicy(context.Background(), policy)
	if err != nil {
		i.logger.Error("minio-remove-policy-error", err)
		return err
	}

	i.logger.Debug("delete-policy", lager.Data{"output": "removed"})

	return nil
}

func (i *MinioUser) ListAttachedUserPolicies(userName, iamPath string) ([]string, error) {
	var userPolicies []string

	i.logger.Debug("list-attached-user-policies", lager.Data{"userName": userName, "iamPath": iamPath})

	userInfo, err := i.madmClnt.GetUserInfo(context.Background(), userName)
	if err != nil {
		i.logger.Error("minio-error", err)
		return userPolicies, err
	}
	i.logger.Debug("list-attached-user-policies", lager.Data{"output": userInfo.PolicyName})

	userPolicies = append(userPolicies, userInfo.PolicyName)
	return userPolicies, nil
}

func (i *MinioUser) AttachUserPolicy(userName, policyARN string) error {
	i.logger.Debug("attach-user-policy", lager.Data{"userName": userName, "policyARN": policyARN})

	policy, err := fromPolicyARN(policyARN)
	if err != nil {
		i.logger.Error("minio-error", err)
		return err
	}

	err = i.madmClnt.SetPolicy(context.Background(), policy, userName, false)
	if err != nil {
		i.logger.Error("minio-error", err)
		return err
	}

	i.logger.Debug("attach-user-policy", lager.Data{"output": "success"})

	return nil
}

func (i *MinioUser) DetachUserPolicy(userName, policyARN string) error {
	i.logger.Debug("detach-user-policy", lager.Data{"userName": userName, "policyARN": policyARN})

	err := i.madmClnt.SetPolicy(context.Background(), "", userName, false)
	if err != nil {
		i.logger.Error("minio-error", err)
		return err
	}

	i.logger.Debug("detach-user-policy", lager.Data{"output": "success"})

	return nil
}

func toUserARN(userName string) string {
	return toARN("user", userName)
}
func toPolicyARN(policyName string) string {
	return toARN("policy", policyName)
}
func toARN(resourceType, resource string) string {
	return fmt.Sprintf("arn:aws:iam:::%s/%s", resourceType, resource)
}

func fromPolicyARN(policyARN string) (string, error) {
	return fromARN("policy", policyARN)
}
func fromARN(resourceType, theARN string) (string, error) {
	// Short-circuit in case we get non-ARN's...
	if !awsarn.IsARN(theARN) {
		return theARN, nil
	}

	arn, err := awsarn.Parse(theARN)
	if err != nil {
		return "", err
	}

	prefix := fmt.Sprintf("%s/", resourceType)
	if !strings.HasPrefix(arn.Resource, prefix) {
		return "", errors.Errorf("Not a %s ARN: %s", resourceType, theARN)
	}

	return strings.ReplaceAll(arn.Resource, prefix, ""), nil
}
