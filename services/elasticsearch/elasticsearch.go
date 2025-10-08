package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/awsiam"
	"github.com/cloud-gov/aws-broker/base"
	jobs "github.com/cloud-gov/aws-broker/jobs"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"

	"fmt"
)

type ElasticsearchAdapter interface {
	createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error)
	checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error)
	bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error)
	deleteElasticsearch(i *ElasticsearchInstance, passoword string, queue *jobs.AsyncJobManager) (base.InstanceState, error)
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
	return i.getCredentials()
}

func (d *mockElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string, queue *jobs.AsyncJobManager) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}

type dedicatedElasticsearchAdapter struct {
	Plan       catalog.ElasticsearchPlan
	settings   config.Settings
	logger     lager.Logger
	iam        awsiam.IAMClientInterface
	sts        STSClientInterface
	opensearch OpensearchClientInterface
	ip         *awsiam.IAMPolicyClient
	s3         S3ClientInterface
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-elasticsearch-broker-"

func (d *dedicatedElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	user := awsiam.NewIAMUserClient(d.iam, d.logger)

	// IAM User and policy before domain starts creating so it can be used to create access control policy
	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
	_, err := user.Create(i.Domain, "", iamTags)
	if err != nil {
		d.logger.Error("createElasticsearch: user.Create err", err)
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
	userResp, _ := d.iam.GetUser(context.TODO(), userParams)
	uniqueUserArn := *(userResp.User.Arn)
	stsInput := &sts.GetCallerIdentityInput{}
	result, err := d.sts.GetCallerIdentity(context.TODO(), stsInput)
	if err != nil {
		d.logger.Error("createElasticsearch: GetCallerIdentity err", err)
		return base.InstanceNotCreated, nil
	}

	accountID := result.Account

	time.Sleep(5 * time.Second)

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"" + uniqueUserArn + "\"},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + d.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	params, err := prepareCreateDomainInput(i, accessControlPolicy)
	if err != nil {
		d.logger.Error("createElasticsearch: prepareCreateDomainInput err", err)
		return base.InstanceNotCreated, err
	}

	resp, err := d.opensearch.CreateDomain(context.TODO(), params)
	if isInvalidTypeException(err) {
		// IAM is eventually consistent, meaning new IAM users may not be immediately available for read, such as when
		// Opensearch goes to validate the IAM user specified as the AWS principal in the access
		// policy. The error returned in this case is an "InvalidTypeException", so if we catch that specific error,
		// we wait for 5 seconds to retry the domain creation to hopefully allow IAM to become consistent.
		//
		// see https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency
		log.Println("Retrying domain creation because of possible IAM eventual consistency issue")
		time.Sleep(5 * time.Second)
		resp, err = d.opensearch.CreateDomain(context.TODO(), params)
	}

	// Decide if AWS service call was successful
	if err != nil {
		d.logger.Error("createElasticsearch: CreateDomain err", err)
		return base.InstanceNotCreated, err
	}

	i.ARN = *(resp.DomainStatus.ARN)
	esARNs := make([]string, 0)
	esARNs = append(esARNs, i.ARN)
	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
	policyARN, err := d.ip.CreatePolicyFromTemplate(i.Domain, "/", policy, esARNs, iamTags)
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

func (d *dedicatedElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
	params, err := prepareUpdateDomainConfigInput(i)
	if err != nil {
		return base.InstanceNotModified, err
	}

	_, err = d.opensearch.UpdateDomainConfig(context.TODO(), params)
	if err != nil {
		d.logger.Error("modifyElasticsearch: UpdateDomainConfig err", err)
		return base.InstanceNotModified, err
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &opensearch.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := d.opensearch.DescribeDomain(context.TODO(), params)
		if err != nil {
			d.logger.Error("bindElasticsearchToApp: UpdateDomainConfig err", err)
			return nil, err
		}

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			if resp.DomainStatus.Endpoints != nil && resp.DomainStatus.ARN != nil {
				fmt.Printf("endpoint: %s ARN: %s \n", resp.DomainStatus.Endpoints["vpc"], *(resp.DomainStatus.ARN))
				i.Host = resp.DomainStatus.Endpoints["vpc"]
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
	return i.getCredentials()
}

// we make the deletion async, set status to in-progress and rollup to return a 202
func (d *dedicatedElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string, queue *jobs.AsyncJobManager) (base.InstanceState, error) {
	//check for backing resource and do async otherwise remove from db
	params := &opensearch.DescribeDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err := d.opensearch.DescribeDomain(context.TODO(), params)
	if err != nil {
		var notFoundException *opensearchTypes.ResourceNotFoundException
		if errors.As(err, &notFoundException) {
			return base.InstanceGone, err
		}

		d.logger.Error("deleteElasticsearch: DescribeDomain error", err)
		return base.InstanceNotGone, err
	}
	// perform async deletion and return in progress
	jobchan, err := queue.RequestJobMessageQueue(i.ServiceID, i.Uuid, base.DeleteOp)
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
		params := &opensearch.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := d.opensearch.DescribeDomain(context.TODO(), params)
		if err != nil {
			d.logger.Error("checkElasticsearchStatus: UpdateDomainConfig err", err)
			return base.InstanceNotCreated, err
		}

		fmt.Printf("domain status: %+v\n", resp.DomainStatus)

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
	}
	return base.InstanceNotCreated, nil

}

// utility to create roles and policies to enable snapshots in an s3 bucket
// we pass bucket-name separately to enable reuse for client and broker buckets
func (d *dedicatedElasticsearchAdapter) createUpdateBucketRolesAndPolicies(
	i *ElasticsearchInstance,
	bucket string,
	path string,
	iamTags []iamTypes.Tag,
) error {
	// ip := awsiam.NewIAMPolicyClient(d.settings.Region, d.logger)
	var snapshotRole *iamTypes.Role

	// create snapshotrole if not done yet
	if i.SnapshotARN == "" {
		rolename := i.Domain + "-to-s3-SnapshotRole"
		policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		arole, err := d.ip.CreateAssumeRole(policy, rolename, iamTags)
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
		policyarn, err := d.ip.CreateUserPolicy(policy, policyname, username, iamTags)
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
		policyarn, err := d.ip.CreatePolicyAttachRole(policyname, policy, *snapshotRole, iamTags)
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- CreatePolicyAttachRole Error", err)
			return err
		}
		i.SnapshotPolicyARN = policyarn

	} else {
		// snaphost policy has already been created so we need to add the new statements for this new bucket
		// to the existing policy version.
		_, err := d.ip.UpdateExistingPolicy(i.SnapshotPolicyARN, []awsiam.PolicyStatementEntry{listStatement, objectStatement})
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- UpdateExistingPolicy Error", err)
			return err
		}

	}
	return nil
}

// state is persisted in the jobs for LastOperations polling.
func (d *dedicatedElasticsearchAdapter) asyncDeleteElasticSearchDomain(i *ElasticsearchInstance, password string, jobstate chan jobs.AsyncJobMsg) {
	defer close(jobstate)

	msg := jobs.AsyncJobMsg{
		BrokerId:   i.ServiceID,
		InstanceId: i.Uuid,
		JobType:    base.DeleteOp,
		JobState:   jobs.AsyncJobState{},
	}
	msg.JobState.Message = fmt.Sprintf("Async DeleteOperation Started for Service Instance: %s", i.Uuid)
	msg.JobState.State = base.InstanceInProgress
	jobstate <- msg

	err := d.takeLastSnapshot(i, password)
	if err != nil {
		desc := fmt.Sprintf("asyncDeleteElasticSearchDomain - \t takeLastSnapshot returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	err = d.writeManifestToS3(i)
	if err != nil {
		desc := fmt.Sprintf("asyncDeleteElasticSearchDomain - \t writeManifestToS3 returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	err = d.cleanupRolesAndPolicies(i)
	if err != nil {
		desc := fmt.Sprintf("asyncDeleteElasticSearchDomain - \t cleanupRolesAndPolicies returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}

	err = d.cleanupElasticSearchDomain(i)
	if err != nil {
		desc := fmt.Sprintf("asyncDeleteElasticSearchDomain - \t cleanupElasticSearchDomain returned error: %v\n", err)
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
		creds, err = i.getCredentials()
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
		if res == "SUCCESS" {
			break
		}
		time.Sleep(sleep)
	}
	return nil
}

// in which we clean up all the roles and policies for the ES domain
func (d *dedicatedElasticsearchAdapter) cleanupRolesAndPolicies(i *ElasticsearchInstance) error {
	user := awsiam.NewIAMUserClient(d.iam, d.logger)

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

	if _, err := d.iam.DetachRolePolicy(context.TODO(), roleDetachPolicyInput); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := d.ip.DeletePolicy(i.SnapshotPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	rolePolicyDeleteInput := &iam.DeleteRoleInput{
		RoleName: aws.String(i.Domain + "-to-s3-SnapshotRole"),
	}

	if _, err := d.iam.DeleteRole(context.TODO(), rolePolicyDeleteInput); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := d.ip.DeletePolicy(i.IamPassRolePolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := user.Delete(i.Domain); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := d.ip.DeletePolicy(i.IamPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

// in which we finally delete the ES Domain and wait for it to complete
func (d *dedicatedElasticsearchAdapter) cleanupElasticSearchDomain(i *ElasticsearchInstance) error {
	params := &opensearch.DeleteDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err := d.opensearch.DeleteDomain(context.TODO(), params)
	if err != nil {
		return err
	}

	// now we poll for completion
	// TODO - don't allow polling forever
	for {
		time.Sleep(time.Minute)
		params := &opensearch.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		_, err := d.opensearch.DescribeDomain(context.TODO(), params)
		if err != nil {
			var notFoundException *opensearchTypes.ResourceNotFoundException
			if errors.As(err, &notFoundException) {
				// Instance no longer exists, this is success
				d.logger.Info(fmt.Sprintf("%s domain has been deleted", i.Domain))
				return nil
			}

			d.logger.Error("cleanupElasticSearchDomain: DescribeDomain err", err)
			return err
		}
	}
}

// in which we Marshall the instance into Json and dump to a manifest file in the snapshot bucket
// so to provide machine readable information for restoration.
func (d *dedicatedElasticsearchAdapter) writeManifestToS3(i *ElasticsearchInstance) error {
	//  marshall instance to bytes.
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)

	serverSideEncryption, err := getS3ServerSideEncryptionEnum("AES256")
	if err != nil {
		return err
	}

	// put json blob into object in s3
	input := s3.PutObjectInput{
		Body:                 body,
		Bucket:               aws.String(d.settings.SnapshotsBucketName),
		Key:                  aws.String(i.SnapshotPath + "/instance_manifest.json"),
		ServerSideEncryption: *serverSideEncryption,
	}

	_, err = d.s3.PutObject(context.TODO(), &input)
	if err != nil {
		d.logger.Error("writeManifesttoS3: PutObject err", err)
		return err
	}

	return nil
}

// determine whether the error is an opensearch.InvalidTypeException
func isInvalidTypeException(createErr error) bool {
	var InvalidTypeException *opensearchTypes.InvalidTypeException
	return errors.As(createErr, &InvalidTypeException)
}

func prepareCreateDomainInput(
	i *ElasticsearchInstance,
	accessControlPolicy string,
) (*opensearch.CreateDomainInput, error) {
	elasticsearchTags := ConvertTagsToOpensearchTags(i.Tags)

	volumeType, err := getOpensearchVolumeTypeEnum(i.VolumeType)
	if err != nil {
		return nil, err
	}

	instanceType, err := getOpensearchInstanceTypeEnum(i.InstanceType)
	if err != nil {
		return nil, err
	}

	masterInstanceType, err := getOpensearchInstanceTypeEnum(i.MasterInstanceType)
	if err != nil {
		return nil, err
	}

	ebsoptions := &opensearchTypes.EBSOptions{
		EBSEnabled: aws.Bool(true),
		VolumeSize: aws.Int32(int32(i.VolumeSize)),
		VolumeType: *volumeType,
	}

	esclusterconfig := &opensearchTypes.ClusterConfig{
		InstanceType:  *instanceType,
		InstanceCount: aws.Int32(int32(i.DataCount)),
	}
	if i.MasterEnabled {
		esclusterconfig.DedicatedMasterEnabled = aws.Bool(i.MasterEnabled)
		esclusterconfig.DedicatedMasterCount = aws.Int32(int32(i.MasterCount))
		esclusterconfig.DedicatedMasterType = *masterInstanceType
	}

	snapshotOptions := &opensearchTypes.SnapshotOptions{
		AutomatedSnapshotStartHour: aws.Int32(int32(i.AutomatedSnapshotStartHour)),
	}

	nodeOptions := &opensearchTypes.NodeToNodeEncryptionOptions{
		Enabled: aws.Bool(i.NodeToNodeEncryption),
	}

	domainOptions := &opensearchTypes.DomainEndpointOptions{
		EnforceHTTPS: aws.Bool(true),
	}

	encryptionAtRestOptions := &opensearchTypes.EncryptionAtRestOptions{
		Enabled: aws.Bool(i.EncryptAtRest),
	}

	VPCOptions := &opensearchTypes.VPCOptions{
		SecurityGroupIds: []string{
			i.SecGroup,
		},
	}

	AdvancedOptions := make(map[string]string)

	if i.IndicesFieldDataCacheSize != "" {
		AdvancedOptions["indices.fielddata.cache.size"] = i.IndicesFieldDataCacheSize
	}

	if i.IndicesQueryBoolMaxClauseCount != "" {
		AdvancedOptions["indices.query.bool.max_clause_count"] = i.IndicesQueryBoolMaxClauseCount
	}

	if i.DataCount > 1 {
		VPCOptions.SubnetIds = []string{
			i.SubnetID3AZ1,
			i.SubnetID4AZ2,
		}
		esclusterconfig.ZoneAwarenessEnabled = aws.Bool(true)
		azCount := 2 // AZ count MUST match number of subnets, max value is 3
		zoneAwarenessConfig := &opensearchTypes.ZoneAwarenessConfig{
			AvailabilityZoneCount: aws.Int32(int32(azCount)),
		}
		esclusterconfig.ZoneAwarenessConfig = zoneAwarenessConfig
	} else {
		VPCOptions.SubnetIds = []string{
			i.SubnetID2AZ2,
		}
	}

	// Standard Parameters
	params := &opensearch.CreateDomainInput{
		AccessPolicies:              &accessControlPolicy,
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

	return params, nil
}

func prepareUpdateDomainConfigInput(i *ElasticsearchInstance) (*opensearch.UpdateDomainConfigInput, error) {
	AdvancedOptions := make(map[string]string)

	if i.IndicesFieldDataCacheSize != "" {
		AdvancedOptions["indices.fielddata.cache.size"] = i.IndicesFieldDataCacheSize
	}

	if i.IndicesQueryBoolMaxClauseCount != "" {
		AdvancedOptions["indices.query.bool.max_clause_count"] = i.IndicesQueryBoolMaxClauseCount
	}

	params := &opensearch.UpdateDomainConfigInput{
		DomainName:      aws.String(i.Domain),
		AdvancedOptions: AdvancedOptions,
	}

	if i.VolumeSize != 0 && i.VolumeType != "" {
		volumeType, err := getOpensearchVolumeTypeEnum(i.VolumeType)
		if err != nil {
			return nil, err
		}

		params.EBSOptions = &opensearchTypes.EBSOptions{
			EBSEnabled: aws.Bool(true),
			VolumeSize: aws.Int32(int32(i.VolumeSize)),
			VolumeType: *volumeType,
		}
	}

	return params, nil
}
