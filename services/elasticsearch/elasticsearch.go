package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/18F/aws-broker/awsiam"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/taskqueue"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
	"github.com/aws/aws-sdk-go/service/opensearchservice"
	"github.com/aws/aws-sdk-go/service/opensearchservice/opensearchserviceiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"

	"fmt"
)

type ElasticsearchAdapter interface {
	createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error)
	checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error)
	bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error)
	deleteElasticsearch(i *ElasticsearchInstance, passoword string, queue *taskqueue.QueueManager) (base.InstanceState, error)
}

type mockElasticsearchAdapter struct {
}

func (d *mockElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockElasticsearchAdapter) checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error) {
	// TODO
	return base.InstanceReady, nil
}

func (d *mockElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	// TODO
	return i.getCredentials(password)
}

func (d *mockElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string, queue *taskqueue.QueueManager) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}

type dedicatedElasticsearchAdapter struct {
	Plan       catalog.ElasticsearchPlan
	settings   config.Settings
	logger     lager.Logger
	iam        iamiface.IAMAPI
	sts        stsiface.STSAPI
	opensearch opensearchserviceiface.OpenSearchServiceAPI
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-elasticsearch-broker-"

func (d *dedicatedElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	user := awsiam.NewIAMUserClient(d.iam, d.logger)
	ip := awsiam.NewIAMPolicyClient(d.settings.Region, d.logger)

	// IAM User and policy before domain starts creating so it can be used to create access control policy
	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
	_, err := user.Create(i.Domain, "", iamTags)
	if err != nil {
		fmt.Println(err.Error())
		return base.InstanceNotCreated, err
	}
	accessKeyID, secretAccessKey, err := user.CreateAccessKey(i.Domain)
	if err != nil {
		return base.InstanceNotCreated, err
	}
	i.AccessKey = accessKeyID
	i.SecretKey = secretAccessKey

	userParams := &iam.GetUserInput{
		UserName: aws.String(i.Domain),
	}
	userResp, _ := d.iam.GetUser(userParams)
	uniqueUserArn := *(userResp.User.Arn)
	stsInput := &sts.GetCallerIdentityInput{}
	result, err := d.sts.GetCallerIdentity(stsInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return base.InstanceNotCreated, nil
	}

	accountID := result.Account

	time.Sleep(5 * time.Second)

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"" + uniqueUserArn + "\"},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + d.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	params := prepareCreateDomainInput(i, accessControlPolicy)

	resp, err := d.opensearch.CreateDomain(params)
	if isInvalidTypeException(err) {
		// IAM is eventually consistent, meaning new IAM users may not be immediately available for read, such as when
		// Opensearch goes to validate the IAM user specified as the AWS principal in the access
		// policy. The error returned in this case is an "InvalidTypeException", so if we catch that specific error,
		// we wait for 5 seconds to retry the domain creation to hopefully allow IAM to become consistent.
		//
		// see https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency
		log.Println("Retrying domain creation because of possible IAM eventual consistency issue")
		time.Sleep(5 * time.Second)
		resp, err = d.opensearch.CreateDomain(params)
	}

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		i.ARN = *(resp.DomainStatus.ARN)
		esARNs := make([]string, 0)
		esARNs = append(esARNs, i.ARN)
		policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
		policyARN, err := ip.CreatePolicyFromTemplate(i.Domain, "/", policy, esARNs, iamTags)
		if err != nil {
			return base.InstanceNotCreated, err
		}

		if err = user.AttachUserPolicy(i.Domain, policyARN); err != nil {
			return base.InstanceNotCreated, err
		}
		i.IamPolicy = policy
		i.IamPolicyARN = policyARN

		//try setup of roles and policies on create
		err = d.createUpdateBucketRolesAndPolicies(i, d.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
		if err != nil {
			return base.InstanceNotCreated, nil
		}
		i.BrokerSnapshotsEnabled = true
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

func (d *dedicatedElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
	params := prepareUpdateDomainConfigInput(i)

	_, err := d.opensearch.UpdateDomainConfig(params)
	if d.didAwsCallSucceed(err) {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotModified, err
}

func (d *dedicatedElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &opensearchservice.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := d.opensearch.DescribeDomain(params)
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
			return nil, err
		}

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			if resp.DomainStatus.Endpoints != nil && resp.DomainStatus.ARN != nil {
				fmt.Printf("endpoint: %s ARN: %s \n", *(resp.DomainStatus.Endpoints["vpc"]), *(resp.DomainStatus.ARN))
				i.Host = *(resp.DomainStatus.Endpoints["vpc"])
				i.ARN = *(resp.DomainStatus.ARN)
				i.State = base.InstanceReady
				i.CurrentESVersion = *(resp.DomainStatus.EngineVersion)
				// Should only be one regardless. Just return now.
			} else {
				// Something went horribly wrong. Should never get here.
				return nil, errors.New("Invalid memory for endpoint and/or endpoint members.")
			}
		} else {
			// Instance not up yet.
			return nil, errors.New("Instance not available yet. Please wait and try again..")
		}

	}

	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)

	// add broker snapshot bucket and create roles and policies if it hasnt been done.
	if !i.BrokerSnapshotsEnabled {
		if i.SnapshotPath == "" {
			i.SnapshotPath = "/" + i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
		}

		err := d.createUpdateBucketRolesAndPolicies(i, d.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
		if err != nil {
			d.logger.Error("bindElasticsearchToApp - Error in createUpdateRolesAndPolicies", err)
			return nil, err
		}
		i.BrokerSnapshotsEnabled = true
	}

	// add client bucket and adjust policies and roles if present
	if i.Bucket != "" {
		err := d.createUpdateBucketRolesAndPolicies(i, i.Bucket, "", iamTags)
		if err != nil {
			return nil, err
		}
	}
	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(password)
}

// we make the deletion async, set status to in-progress and rollup to return a 202
func (d *dedicatedElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string, queue *taskqueue.QueueManager) (base.InstanceState, error) {
	//check for backing resource and do async otherwise remove from db
	params := &opensearchservice.DescribeDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err := d.opensearch.DescribeDomain(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
			// Instance no longer exists, force a removal from brokerdb
			if awsErr.Code() == opensearchservice.ErrCodeResourceNotFoundException {
				return base.InstanceGone, err
			}
		}
		return base.InstanceNotGone, err
	}
	// perform async deletion and return in progress
	jobchan, err := queue.RequestTaskQueue(i.ServiceID, i.Uuid, base.DeleteOp)
	if err == nil {
		go d.asyncDeleteElasticSearchDomain(i, password, jobchan)
	}
	return base.InstanceInProgress, nil
}

// this should only be called in relation to async create, modify or delete operations polling for completion
func (d *dedicatedElasticsearchAdapter) checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error) {
	// First, we need to check if the instance state
	// Only search for details if the instance was not indicated as ready.

	if i.State != base.InstanceReady {
		params := &opensearchservice.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := d.opensearch.DescribeDomain(params)
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
			return base.InstanceNotCreated, err
		}

		fmt.Println(fmt.Printf("domain status: %s", resp.DomainStatus))

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			switch *(resp.DomainStatus.Processing) {
			case false:
				return base.InstanceReady, nil
			case true:
				return base.InstanceInProgress, nil
			default:
				return base.InstanceInProgress, nil
			}
		} else {
			// Instance not up yet.
			return base.InstanceNotCreated, errors.New("Instance not available yet. Please wait and try again..")
		}
	    return base.InstanceNotCreated, nil

}

func (d *dedicatedElasticsearchAdapter) didAwsCallSucceed(err error) bool {
	// TODO Eventually return a formatted error object.
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
		} else {
			// This case should never be hit, The SDK should alwsy return an
			// error which satisfies the awserr.Error interface.
			fmt.Println(err.Error())
		}
		return false
	}
	return true
}

// utility to create roles and policies to enable snapshots in an s3 bucket
// we pass bucket-name separately to enable reuse for client and broker buckets
func (d *dedicatedElasticsearchAdapter) createUpdateBucketRolesAndPolicies(
	i *ElasticsearchInstance,
	bucket string,
	path string,
	iamTags []*iam.Tag,
) error {
	ip := awsiam.NewIAMPolicyClient(d.settings.Region, d.logger)
	var snapshotRole *iam.Role

	// create snapshotrole if not done yet
	if i.SnapshotARN == "" {
		rolename := i.Domain + "-to-s3-SnapshotRole"
		policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		arole, err := ip.CreateAssumeRole(policy, rolename, iamTags)
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- CreateAssumeRole Error", err)
			return err
		}

		i.SnapshotARN = *arole.Arn
		snapshotRole = arole

	}

	// create PassRolePolicy if DNE
	if i.IamPassRolePolicyARN == "" {
		policy := `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "iam:PassRole","Resource": "` + i.SnapshotARN + `"},{"Effect": "Allow","Action": "es:ESHttpPut","Resource": "` + i.ARN + `/*"}]}`
		policyname := i.Domain + "-to-S3-ESRolePolicy"
		username := i.Domain
		policyarn, err := ip.CreateUserPolicy(policy, policyname, username, iamTags)
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- CreateUserPolicy Error", err)
			return err
		}
		i.IamPassRolePolicyARN = policyarn
	}

	// Create PolicyDoc Statements
	// looks like: {"Action": ["s3:ListBucket"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `"]}
	bucketArn := "arn:aws-us-gov:s3:::" + bucket
	listStatement := awsiam.PolicyStatementEntry{
		Action:   []string{"s3:ListBucket"},
		Effect:   "Allow",
		Resource: []string{bucketArn},
	}
	// add wildcard for any path including empty one
	// using path will now limit access to the specific path provided
	path += "/*"
	// Looks like: {"Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `/*"]}
	objectStatement := awsiam.PolicyStatementEntry{
		Action:   []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject"},
		Effect:   "Allow",
		Resource: []string{bucketArn + path},
	}

	// create s3 access Policy for snapshot role if DNE, else update policy to include another set of statements for this bucket
	if i.SnapshotPolicyARN == "" {

		policyDoc := awsiam.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []awsiam.PolicyStatementEntry{listStatement, objectStatement},
		}

		policyname := i.Domain + "-to-S3-RolePolicy"
		policy, err := policyDoc.ToString()
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- policyDoc.ToString Error", err)
			return err
		}
		policyarn, err := ip.CreatePolicyAttachRole(policyname, policy, *snapshotRole, iamTags)
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- CreatePolicyAttachRole Error", err)
			return err
		}
		i.SnapshotPolicyARN = policyarn

	} else {
		// snaphost policy has already been created so we need to add the new statements for this new bucket
		// to the existing policy version.
		_, err := ip.UpdateExistingPolicy(i.SnapshotPolicyARN, []awsiam.PolicyStatementEntry{listStatement, objectStatement})
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- UpdateExistingPolicy Error", err)
			return err
		}

	}
	return nil
}

// state is persisted in the taskqueue for LastOperations polling.
func (d *dedicatedElasticsearchAdapter) asyncDeleteElasticSearchDomain(i *ElasticsearchInstance, password string, jobstate chan taskqueue.AsyncJobMsg) {
	defer close(jobstate)

	msg := taskqueue.AsyncJobMsg{
		BrokerId:   i.ServiceID,
		InstanceId: i.Uuid,
		JobType:    base.DeleteOp,
		JobState:   taskqueue.AsyncJobState{},
	}
	msg.JobState.Message = fmt.Sprintf("Async DeleteOperation Started for Service Instance: %s", i.Uuid)
	msg.JobState.State = base.InstanceInProgress
	jobstate <- msg

	err := d.takeLastSnapshot(i, password)
	if err != nil {
		desc := fmt.Sprintf("asyncDelete - \n\t takeLastSnapshot returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	err = d.writeManifestToS3(i, password)
	if err != nil {
		desc := fmt.Sprintf("asyncDelete - \n\t writeManifestToS3 returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	err = d.cleanupRolesAndPolicies(i)
	if err != nil {
		desc := fmt.Sprintf("asyncDelete - \n\t cleanupRolesAndPolicies returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	err = d.cleanupElasticSearchDomain(i)
	if err != nil {
		desc := fmt.Sprintf("asyncDelete - \n\t cleanupElasticSearchDomain returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	msg.JobState.Message = fmt.Sprintf("Async DeleteOperation Completed for Service Instance: %s", i.Uuid)
	msg.JobState.State = base.InstanceGone
	jobstate <- msg
}

// in which we make the ES API call to take a snapshot
// then poll for snapshot completetion, may block for a considerable time
func (d *dedicatedElasticsearchAdapter) takeLastSnapshot(i *ElasticsearchInstance, password string) error {

	var sleep = 10 * time.Second
	var creds map[string]string
	var err error

	// check if instance was never bound and thus never set host...
	if i.Host == "" {
		creds, err = d.bindElasticsearchToApp(i, password)
		if err != nil {
			fmt.Println(err)
			return err
		}
	} else {
		creds, err = i.getCredentials(password)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	// add broker snapshot bucket and create roles and policies if it hasnt been done.
	if !i.BrokerSnapshotsEnabled {
		if i.SnapshotPath == "" {
			i.SnapshotPath = "/" + i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
		}
		iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
		err := d.createUpdateBucketRolesAndPolicies(i, d.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
		if err != nil {
			d.logger.Error("bindElasticsearchToApp - Error in createUpdateRolesAndPolicies", err)
			return err
		}
		i.BrokerSnapshotsEnabled = true
	}

	// EsApiHandler takes care of v4 signing of requests, and other header/ request formation.
	esApi := &EsApiHandler{}
	esApi.Init(creds, d.settings.Region)

	// create snapshot repo
	_, err = esApi.CreateSnapshotRepo(
		d.settings.SnapshotsRepoName,
		d.settings.SnapshotsBucketName,
		i.SnapshotPath,
		d.settings.Region,
		i.SnapshotARN,
	)
	if err != nil {
		d.logger.Error("createsnapshotrepo returns error", err)
		return err
	}

	// create snapshot
	_, err = esApi.CreateSnapshot(d.settings.SnapshotsRepoName, d.settings.LastSnapshotName)
	if err != nil {
		d.logger.Error("CreateSnapshot returns error", err)
		return err
	}

	// poll for snapshot completion and continue once no longer "IN_PROGRESS"
	for {
		res, err := esApi.GetSnapshotStatus(d.settings.SnapshotsRepoName, d.settings.LastSnapshotName)
		if err != nil {
			d.logger.Error("GetSnapShotStatus failed", err)
			return err
		}
		if res != "IN_PROGRESS" {
			break
		}
		time.Sleep(sleep)
	}
	return nil
}

// in which we clean up all the roles and policies for the ES domain
func (d *dedicatedElasticsearchAdapter) cleanupRolesAndPolicies(i *ElasticsearchInstance) error {
	user := awsiam.NewIAMUserClient(d.iam, d.logger)
	policyHandler := awsiam.NewIAMPolicyClient(d.settings.Region, d.logger)

	if err := user.DetachUserPolicy(i.Domain, i.IamPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := user.DeleteAccessKey(i.Domain, i.AccessKey); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := user.DetachUserPolicy(i.Domain, i.IamPassRolePolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	roleDetachPolicyInput := &iam.DetachRolePolicyInput{
		PolicyArn: aws.String(i.SnapshotPolicyARN),
		RoleName:  aws.String(i.Domain + "-to-s3-SnapshotRole"),
	}

	if _, err := d.iam.DetachRolePolicy(roleDetachPolicyInput); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := policyHandler.DeletePolicy(i.SnapshotPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	rolePolicyDeleteInput := &iam.DeleteRoleInput{
		RoleName: aws.String(i.Domain + "-to-s3-SnapshotRole"),
	}

	if _, err := d.iam.DeleteRole(rolePolicyDeleteInput); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := policyHandler.DeletePolicy(i.IamPassRolePolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := user.Delete(i.Domain); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := policyHandler.DeletePolicy(i.IamPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

// in which we finally delete the ES Domain and wait for it to complete
func (d *dedicatedElasticsearchAdapter) cleanupElasticSearchDomain(i *ElasticsearchInstance) error {
	params := &opensearchservice.DeleteDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	resp, err := d.opensearch.DeleteDomain(params)

	// Pretty-print the response data.
	d.logger.Info(fmt.Sprintf("aws.DeleteElasticSearchDomain: \n\t%s\n", awsutil.StringValue(resp)))

	// Decide if AWS service call was successful
	if success := d.didAwsCallSucceed(err); !success {
		return err
	}
	// now we poll for completion
	// TODO - don't allow polling forever
	for {
		time.Sleep(time.Minute)
		params := &opensearchservice.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		_, err := d.opensearch.DescribeDomain(params)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				// Instance no longer exists, this is success
				if awsErr.Code() == opensearchservice.ErrCodeResourceNotFoundException {
					d.logger.Info(fmt.Sprintf("%s domain has been deleted", i.Domain))
					return nil
				}
				// Generic AWS error with Code, Message, and original error (if any)
				d.logger.Error("CleanUpESDomain - svc.DescribeElasticSearchDomain Failed", awsErr)

				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// A service error occurred
					fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				}
			}
			return err
		}
	}
}

// in which we Marshall the instance into Json and dump to a manifest file in the snapshot bucket
// so to provide machine readable information for restoration.
func (d *dedicatedElasticsearchAdapter) writeManifestToS3(i *ElasticsearchInstance, password string) error {
	//  marshall instance to bytes.
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)

	session, err := session.NewSession(&aws.Config{
		Region: aws.String(d.settings.Region),
	})
	if err != nil {
		d.logger.Error("writeManifesttoS3.NewSession failed", err)
		return err
	}

	// put json blob into object in s3
	svc := s3.New(session)
	input := s3.PutObjectInput{
		Body:                 body,
		Bucket:               aws.String(d.settings.SnapshotsBucketName),
		Key:                  aws.String(i.SnapshotPath + "/instance_manifest.json"),
		ServerSideEncryption: aws.String("AES256"),
	}

	_, err = svc.PutObject(&input)
	// Decide if AWS service call was successful
	if success := d.didAwsCallSucceed(err); !success {
		d.logger.Error("writeManifesttoS3.PutObject Failed", err)
		return err
	}
	return nil
}

// determine whether the error is an opensearchservice.InvalidTypeException
func isInvalidTypeException(createErr error) bool {
	if aerr, ok := createErr.(awserr.Error); ok {
		return aerr.Code() == "InvalidTypeException"
	}
	return false
}

func prepareCreateDomainInput(
	i *ElasticsearchInstance,
	accessControlPolicy string,
) *opensearchservice.CreateDomainInput {
	elasticsearchTags := ConvertTagsToOpensearchTags(i.Tags)

	ebsoptions := &opensearchservice.EBSOptions{
		EBSEnabled: aws.Bool(true),
		VolumeSize: aws.Int64(int64(i.VolumeSize)),
		VolumeType: aws.String(i.VolumeType),
	}

	esclusterconfig := &opensearchservice.ClusterConfig{
		InstanceType:  aws.String(i.InstanceType),
		InstanceCount: aws.Int64(int64(i.DataCount)),
	}
	if i.MasterEnabled {
		esclusterconfig.SetDedicatedMasterEnabled(i.MasterEnabled)
		esclusterconfig.SetDedicatedMasterCount(int64(i.MasterCount))
		esclusterconfig.SetDedicatedMasterType(i.MasterInstanceType)
	}

	snapshotOptions := &opensearchservice.SnapshotOptions{
		AutomatedSnapshotStartHour: aws.Int64(int64(i.AutomatedSnapshotStartHour)),
	}

	nodeOptions := &opensearchservice.NodeToNodeEncryptionOptions{
		Enabled: aws.Bool(i.NodeToNodeEncryption),
	}

	domainOptions := &opensearchservice.DomainEndpointOptions{
		EnforceHTTPS: aws.Bool(true),
	}

	encryptionAtRestOptions := &opensearchservice.EncryptionAtRestOptions{
		Enabled: aws.Bool(i.EncryptAtRest),
	}

	VPCOptions := &opensearchservice.VPCOptions{
		SecurityGroupIds: []*string{
			&i.SecGroup,
		},
	}

	AdvancedOptions := make(map[string]*string)

	if i.IndicesFieldDataCacheSize != "" {
		AdvancedOptions["indices.fielddata.cache.size"] = &i.IndicesFieldDataCacheSize
	}

	if i.IndicesQueryBoolMaxClauseCount != "" {
		AdvancedOptions["indices.query.bool.max_clause_count"] = &i.IndicesQueryBoolMaxClauseCount
	}

	if i.DataCount > 1 {
		VPCOptions.SetSubnetIds([]*string{
			&i.SubnetID3AZ1,
			&i.SubnetID4AZ2,
		})
		esclusterconfig.SetZoneAwarenessEnabled(true)
		azCount := 2 // AZ count MUST match number of subnets, max value is 3
		zoneAwarenessConfig := &opensearchservice.ZoneAwarenessConfig{
			AvailabilityZoneCount: aws.Int64(int64(azCount)),
		}
		esclusterconfig.SetZoneAwarenessConfig(zoneAwarenessConfig)
	} else {
		VPCOptions.SetSubnetIds([]*string{
			&i.SubnetID2AZ2,
		})
	}

	// Standard Parameters
	params := &opensearchservice.CreateDomainInput{
		DomainName:                  aws.String(i.Domain),
		EBSOptions:                  ebsoptions,
		ClusterConfig:               esclusterconfig,
		SnapshotOptions:             snapshotOptions,
		NodeToNodeEncryptionOptions: nodeOptions,
		DomainEndpointOptions:       domainOptions,
		EncryptionAtRestOptions:     encryptionAtRestOptions,
		VPCOptions:                  VPCOptions,
		TagList:                     elasticsearchTags,
	}

	if len(AdvancedOptions) > 0 {
		params.AdvancedOptions = AdvancedOptions
	}

	if i.ElasticsearchVersion != "" {
		params.EngineVersion = aws.String(i.ElasticsearchVersion)
	}

	params.SetAccessPolicies(accessControlPolicy)
	return params
}

func prepareUpdateDomainConfigInput(i *ElasticsearchInstance) *opensearchservice.UpdateDomainConfigInput {
	AdvancedOptions := make(map[string]*string)

	if i.IndicesFieldDataCacheSize != "" {
		AdvancedOptions["indices.fielddata.cache.size"] = &i.IndicesFieldDataCacheSize
	}

	if i.IndicesQueryBoolMaxClauseCount != "" {
		AdvancedOptions["indices.query.bool.max_clause_count"] = &i.IndicesQueryBoolMaxClauseCount
	}

	params := &opensearchservice.UpdateDomainConfigInput{
		DomainName:      aws.String(i.Domain),
		AdvancedOptions: AdvancedOptions,
	}

	if i.VolumeSize != 0 && i.VolumeType != "" {
		params.EBSOptions = &opensearchservice.EBSOptions{
			EBSEnabled: aws.Bool(true),
			VolumeSize: aws.Int64(int64(i.VolumeSize)),
			VolumeType: aws.String(i.VolumeType),
		}
	}

	return params
}
