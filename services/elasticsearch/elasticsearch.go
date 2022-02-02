package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/taskqueue"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/cloudfoundry-community/s3-broker/awsiam"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/iampolicy"

	"fmt"
)

type ElasticsearchAdapter interface {
	createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	modifyElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
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

func (d *mockElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
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

/* type sharedElasticsearchAdapter struct {
	SharedElasticsearchConn *gorm.DB
}

func (d *sharedElasticsearchAdapter) createDB(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *sharedElasticsearchAdapter) bindDBToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	return i.getCredentials(password)
}

func (d *sharedElasticsearchAdapter) deleteRedis(i *ElasticsearchInstance) (base.InstanceState, error) {
	return base.InstanceGone, nil
} */

type dedicatedElasticsearchAdapter struct {
	Plan     catalog.ElasticsearchPlan
	settings config.Settings
	logger   lager.Logger
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-elasticsearch-broker-"

func (d *dedicatedElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	iamsvc := iam.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	logger := lager.NewLogger("aws-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))
	user := awsiam.NewIAMUser(iamsvc, logger)
	stssvc := sts.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	// IAM User and policy before domain starts creating so it can be used to create access control policy
	_, err := user.Create(i.Domain, "")
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
	userResp, _ := iamsvc.GetUser(userParams)
	uniqueUser := *(userResp.User.UserId)
	stsInput := &sts.GetCallerIdentityInput{}
	result, err := stssvc.GetCallerIdentity(stsInput)
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

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": [\"" + uniqueUser + "\"]},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + d.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	var elasticsearchTags []*elasticsearchservice.Tag
	time.Sleep(5 * time.Second)

	for k, v := range i.Tags {
		tag := elasticsearchservice.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}

		elasticsearchTags = append(elasticsearchTags, &tag)
	}

	ebsoptions := &elasticsearchservice.EBSOptions{
		EBSEnabled: aws.Bool(true),
		VolumeSize: aws.Int64(int64(i.VolumeSize)),
		VolumeType: aws.String(i.VolumeType),
	}

	zoneAwarenessConfig := &elasticsearchservice.ZoneAwarenessConfig{
		AvailabilityZoneCount: aws.Int64(2),
	}

	esclusterconfig := &elasticsearchservice.ElasticsearchClusterConfig{
		InstanceType:  aws.String(i.InstanceType),
		InstanceCount: aws.Int64(int64(i.DataCount)),
	}
	if i.MasterEnabled {
		esclusterconfig.SetDedicatedMasterEnabled(i.MasterEnabled)
		esclusterconfig.SetDedicatedMasterCount(int64(i.MasterCount))
		esclusterconfig.SetDedicatedMasterType(i.MasterInstanceType)
	}

	if i.DataCount > 1 {
		esclusterconfig.SetZoneAwarenessEnabled(true)
		esclusterconfig.SetZoneAwarenessConfig(zoneAwarenessConfig)
	}

	log.Println(fmt.Sprint(i.MasterCount))

	snapshotOptions := &elasticsearchservice.SnapshotOptions{
		AutomatedSnapshotStartHour: aws.Int64(int64(i.AutomatedSnapshotStartHour)),
	}

	nodeOptions := &elasticsearchservice.NodeToNodeEncryptionOptions{
		Enabled: aws.Bool(i.NodeToNodeEncryption),
	}

	domainOptions := &elasticsearchservice.DomainEndpointOptions{
		EnforceHTTPS: aws.Bool(true),
	}

	encryptionAtRestOptions := &elasticsearchservice.EncryptionAtRestOptions{
		Enabled: aws.Bool(i.EncryptAtRest),
	}

	VPCOptions := &elasticsearchservice.VPCOptions{
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
			&i.SubnetIDAZ1,
			&i.SubnetIDAZ2,
		})
	} else {
		VPCOptions.SetSubnetIds([]*string{
			&i.SubnetIDAZ1,
		})
	}

	// Standard Parameters
	params := &elasticsearchservice.CreateElasticsearchDomainInput{
		DomainName:                  aws.String(i.Domain),
		ElasticsearchVersion:        aws.String(i.ElasticsearchVersion),
		EBSOptions:                  ebsoptions,
		ElasticsearchClusterConfig:  esclusterconfig,
		SnapshotOptions:             snapshotOptions,
		NodeToNodeEncryptionOptions: nodeOptions,
		DomainEndpointOptions:       domainOptions,
		EncryptionAtRestOptions:     encryptionAtRestOptions,
		VPCOptions:                  VPCOptions,
		AdvancedOptions:             AdvancedOptions,
	}

	params.SetAccessPolicies(accessControlPolicy)

	resp, err := svc.CreateElasticsearchDomain(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))
	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		i.ARN = *(resp.DomainStatus.ARN)
		esARNs := make([]string, 0)
		esARNs = append(esARNs, i.ARN)
		policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
		policyARN, err := user.CreatePolicy(i.Domain, "/", policy, esARNs)
		if err != nil {
			return base.InstanceNotCreated, err
		}

		if err = user.AttachUserPolicy(i.Domain, policyARN); err != nil {
			return base.InstanceNotCreated, err
		}
		i.IamPolicy = policy
		i.IamPolicyARN = policyARN
		paramsTags := &elasticsearchservice.AddTagsInput{
			TagList: elasticsearchTags,
			ARN:     resp.DomainStatus.ARN,
		}
		resp1, err := svc.AddTags(paramsTags)
		fmt.Println(awsutil.StringValue(resp1))
		if !d.didAwsCallSucceed(err) {
			return base.InstanceNotCreated, nil
		}
		// try setup of roles and policies on create
		err = d.createUpdateBucketRolesAndPolicies(i, d.settings.SnapshotsBucketName, i.SnapshotPath)
		if err != nil {
			return base.InstanceNotCreated, nil
		}
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

func (d *dedicatedElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	AdvancedOptions := make(map[string]*string)

	if i.IndicesFieldDataCacheSize != "" {
		AdvancedOptions["indices.fielddata.cache.size"] = &i.IndicesFieldDataCacheSize
	}

	if i.IndicesQueryBoolMaxClauseCount != "" {
		AdvancedOptions["indices.query.bool.max_clause_count"] = &i.IndicesQueryBoolMaxClauseCount
	}
	// Standard Parameters
	params := &elasticsearchservice.UpdateElasticsearchDomainConfigInput{
		DomainName:      aws.String(i.Domain),
		AdvancedOptions: AdvancedOptions,
	}
	resp, err := svc.UpdateElasticsearchDomainConfig(params)
	fmt.Println(awsutil.StringValue(resp))
	if d.didAwsCallSucceed(err) {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotModified, err
}

func (d *dedicatedElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &elasticsearchservice.DescribeElasticsearchDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := svc.DescribeElasticsearchDomain(params)
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

		// Pretty-print the response data.
		fmt.Println(awsutil.StringValue(resp))

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			if resp.DomainStatus.Endpoints != nil && resp.DomainStatus.ARN != nil {
				fmt.Printf("endpoint: %s ARN: %s \n", *(resp.DomainStatus.Endpoints["vpc"]), *(resp.DomainStatus.ARN))
				i.Host = *(resp.DomainStatus.Endpoints["vpc"])
				i.ARN = *(resp.DomainStatus.ARN)
				i.State = base.InstanceReady
				i.CurrentESVersion = *(resp.DomainStatus.ElasticsearchVersion)
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

	// add broker snapshot bucket and create roles and policies if it hasnt been done.
	if d.settings.SnapshotsBucketName != "" && !i.BrokerSnapshotsEnabled {

		err := d.createUpdateBucketRolesAndPolicies(i, d.settings.SnapshotsBucketName, i.SnapshotPath)
		if err != nil {
			d.logger.Error("bindElasticsearchToApp - Error in createUpdateRolesAndPolicies", err)
			return nil, err
		}

		err = d.createSnapshotRepo(i, password, d.settings.SnapshotsBucketName, i.SnapshotPath, d.settings.Region)
		if err != nil {
			d.logger.Error("bindElasticsearchToApp - Error in createSnapshotRepo", err)
			return nil, err
		}
		i.BrokerSnapshotsEnabled = true
	}

	// add client bucket and adjust policies and roles if present
	if i.Bucket != "" {
		err := d.createUpdateBucketRolesAndPolicies(i, i.Bucket, "")
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
	svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	params := &elasticsearchservice.DescribeElasticsearchDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err := svc.DescribeElasticsearchDomain(params)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS error with Code, Message, and original error (if any)
			fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
			}
			// Instance no longer exists, force a removal from brokerdb
			if awsErr.Code() == elasticsearchservice.ErrCodeResourceNotFoundException {
				d.logger.Debug("DeleteES - No backing resource found, returning InstanceGone")
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
		svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &elasticsearchservice.DescribeElasticsearchDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := svc.DescribeElasticsearchDomain(params)
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

		// Pretty-print the response data.
		fmt.Println(awsutil.StringValue(resp))

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

// utility to run native api calls on ES instance to create a snapshot repository using the bucket name provided
func (d *dedicatedElasticsearchAdapter) createSnapshotRepo(i *ElasticsearchInstance, password string, bucket string, path string, region string) error {

	creds, err := i.getCredentials(password)
	if err != nil {
		d.logger.Error("createSnapshotRepo.getCredentials failed", err)
		return err
	}
	// EsApiHandler takes care of v4 signing of requests, and other header/ request formation.
	esApi := &EsApiHandler{}
	esApi.Init(creds, region)

	// create snapshot repo
	resp, err := esApi.CreateSnapshotRepo(d.settings.SnapshotsRepoName, bucket, path, region, i.SnapshotARN)
	if err != nil {
		d.logger.Error("createsnapshotrepo returns error", err)
		return err
	}
	d.logger.Debug(fmt.Sprintf("createSnapshotRepo returns response:\n %v\n", resp))
	return nil
}

// utility to create roles and policies to enable snapshots in an s3 bucket
// we pass bucket-name separately to enable reuse for client and broker buckets
func (d *dedicatedElasticsearchAdapter) createUpdateBucketRolesAndPolicies(i *ElasticsearchInstance, bucket string, path string) error {
	d.logger.Debug(fmt.Sprintf("createUpdateBucketRolesAndPolcies -- bucket: %s path: %s\n", bucket, path))
	ip := iampolicy.NewIamPolicyHandler(d.settings.Region)
	var snapshotRole *iam.Role

	// create snapshotrole if not done yet
	if i.SnapshotARN == "" {
		rolename := i.Domain + "-to-s3-SnapshotRole"
		policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		arole, err := ip.CreateAssumeRole(policy, rolename)
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
		policyarn, err := ip.CreateUserPolicy(policy, policyname, username)
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- CreateUserPolicy Error", err)
			return err
		}
		i.IamPassRolePolicyARN = policyarn
	}

	// Create PolicyDoc Statements
	// looks like: {"Action": ["s3:ListBucket"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `"]}
	bucketArn := "arn:aws-us-gov:s3:::" + bucket
	listStatement := iampolicy.PolicyStatementEntry{
		Action:   []string{"s3:ListBucket"},
		Effect:   "Allow",
		Resource: []string{bucketArn},
	}
	// add wildcard for any path including empty one
	// using path will now limit access to the specific path provided
	path += "/*"
	// Looks like: {"Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `/*"]}
	objectStatement := iampolicy.PolicyStatementEntry{
		Action:   []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject"},
		Effect:   "Allow",
		Resource: []string{bucketArn + path},
	}

	// create s3 access Policy for snapshot role if DNE, else update policy to include another set of statements for this bucket
	if i.SnapshotPolicyARN == "" {

		policyDoc := iampolicy.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []iampolicy.PolicyStatementEntry{listStatement, objectStatement},
		}

		policyname := i.Domain + "-to-S3-RolePolicy"
		policy, err := policyDoc.ToString()
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- policyDoc.ToString Error", err)
			return err
		}
		policyarn, err := ip.CreatePolicyAttachRole(policyname, policy, *snapshotRole)
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- CreatePolicyAttachRole Error", err)
			return err
		}
		i.SnapshotPolicyARN = policyarn

	} else {
		//snaphostpolicy has already be created so we need to add the new statements for this new bucket
		//to the existing policy version.
		_, err := ip.UpdateExistingPolicy(i.SnapshotPolicyARN, []iampolicy.PolicyStatementEntry{listStatement, objectStatement})
		if err != nil {
			d.logger.Error("createUpdateBucketRolesAndPolcies -- UpdateExistingPolicy Error", err)
			return err
		}

	}
	return nil
}

// state is persisted in the taskqueue for LastOperations polling.
func (d *dedicatedElasticsearchAdapter) asyncDeleteElasticSearchDomain(i *ElasticsearchInstance, password string, jobstate chan taskqueue.AsyncJobMsg) {
	//fmt.Printf("\nAsyncDeleteESDomain -- Started\n")
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

	//fmt.Printf("\nAsyncDeleteESDomain -- TakeLastSnapShot Started\n")
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

	//fmt.Printf("\nAsyncDeleteESDomain -- TakeLastSnapShot Completed\n")
	//fmt.Printf("\nAsyncDeleteESDomain -- cleanupRolesAndPolicies Started\n")
	err = d.cleanupRolesAndPolicies(i)
	if err != nil {
		desc := fmt.Sprintf("asyncDelete - \n\t cleanupRolesAndPolicies returned error: %v\n", err)
		fmt.Println(desc)
		msg.JobState.State = base.InstanceNotGone
		msg.JobState.Message = desc
		jobstate <- msg
		return
	}
	//fmt.Printf("\nAsyncDeleteESDomain -- cleanupRolesAndPolicies Completed\n")
	//fmt.Printf("\nAsyncDeleteESDomain -- cleanupElasticSearchDomain Started\n")
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
	//fmt.Printf("\nAsyncDeleteESDomain -- Completed\n")
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
	// // catch legacy domains that dont have snapshotbucket configured yet and create the iam policies and repo
	// //fmt.Printf("TakeLastSnapshot - \n\tbucketname: %s\n\tbrokersnapshot enabled: %v\n", d.settings.SnapshotsBucketName, i.BrokerSnapshotsEnabled)
	// if d.settings.SnapshotsBucketName != "" && !i.BrokerSnapshotsEnabled {
	// 	fmt.Println("TakeLastSnapshot - Creating Policies and Repo ")

	// 	err := d.createUpdateBucketRolesAndPolicies(i, d.settings.SnapshotsBucketName, i.SnapshotPath)
	// 	if err != nil {
	// 		d.logger.Error("TakeLastSnapshot - Error in createUpdateRolesAndPolicies", err)
	// 		return err
	// 	}
	// 	err = d.createSnapshotRepo(i, password, d.settings.SnapshotsBucketName, i.SnapshotPath, d.settings.Region)
	// 	if err != nil {
	// 		d.logger.Error("TakeLastSnapshot - Error in createSnapshotRepo", err)
	// 		return err
	// 	}
	// 	i.BrokerSnapshotsEnabled = true
	// }
	// exec snapshot request
	// creds, err := i.getCredentials(password)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return err
	// }

	// EsApiHandler takes care of v4 signing of requests, and other header/ request formation.
	esApi := &EsApiHandler{}
	esApi.Init(creds, d.settings.Region)
	//fmt.Println("TakeLastSnapshot - Creating Snapshot")
	// create snapshot
	res, err := esApi.CreateSnapshot(d.settings.SnapshotsRepoName, d.settings.LastSnapshotName)
	if err != nil {
		d.logger.Error("CreateSnapshot returns error", err)
		return err
	}
	d.logger.Debug(fmt.Sprintf("TakeLastSnapshot - \n\tSnapshot Response: %v\n", res))
	// poll for snapshot completion and continue once no longer "IN_PROGRESS"
	for {
		//fmt.Println("TakeLastSnapshot - Polling for Snapshot Completion")
		res, err := esApi.GetSnapshotStatus(d.settings.SnapshotsRepoName, d.settings.LastSnapshotName)
		if err != nil {
			d.logger.Error("GetSnapShotStatus failed", err)
			return err
		}
		if res != "IN_PROGRESS" {
			//fmt.Printf("TakeLastSnapshot - \nSnapshot finished, response %v\n", res)
			break
		}
		//fmt.Printf("Snapshot in progress, waiting for %s\n", sleep)
		time.Sleep(sleep)
	}
	//fmt.Println("TakeLastSnapshot - Completed")
	return nil
}

//in which we clean up all the roles and policies for the ES domain
func (d *dedicatedElasticsearchAdapter) cleanupRolesAndPolicies(i *ElasticsearchInstance) error {
	iamsvc := iam.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	logger := lager.NewLogger("aws-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))
	user := awsiam.NewIAMUser(iamsvc, logger)

	if err := user.DetachUserPolicy(i.Domain, i.IamPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := user.DeleteAccessKey(i.Domain, i.AccessKey); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if i.BrokerSnapshotsEnabled || i.Bucket != "" {
		if err := user.DetachUserPolicy(i.Domain, i.IamPassRolePolicyARN); err != nil {
			fmt.Println(err.Error())
			return err
		}

		roleDetachPolicyInput := &iam.DetachRolePolicyInput{
			PolicyArn: aws.String(i.SnapshotPolicyARN),
			RoleName:  aws.String(i.Domain + "-to-s3-SnapshotRole"),
		}

		if _, err := iamsvc.DetachRolePolicy(roleDetachPolicyInput); err != nil {
			fmt.Println(err.Error())
			return err
		}

		if err := user.DeletePolicy(i.SnapshotPolicyARN); err != nil {
			fmt.Println(err.Error())
			return err
		}

		rolePolicyDeleteInput := &iam.DeleteRoleInput{
			RoleName: aws.String(i.Domain + "-to-s3-SnapshotRole"),
		}

		if _, err := iamsvc.DeleteRole(rolePolicyDeleteInput); err != nil {
			fmt.Println(err.Error())
			return err
		}

		if err := user.DeletePolicy(i.IamPassRolePolicyARN); err != nil {
			fmt.Println(err.Error())
			return err
		}
	}

	if err := user.Delete(i.Domain); err != nil {
		fmt.Println(err.Error())
		return err
	}

	if err := user.DeletePolicy(i.IamPolicyARN); err != nil {
		fmt.Println(err.Error())
		return err
	}
	return nil
}

// in which we finally delete the ES Domain and wait for it to complete
func (d *dedicatedElasticsearchAdapter) cleanupElasticSearchDomain(i *ElasticsearchInstance) error {
	svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	params := &elasticsearchservice.DeleteElasticsearchDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	resp, err := svc.DeleteElasticsearchDomain(params)

	// Pretty-print the response data.
	d.logger.Info(fmt.Sprintf("aws.DeleteElasticSearchDomain: \n\t%s\n", awsutil.StringValue(resp)))

	// Decide if AWS service call was successful
	if success := d.didAwsCallSucceed(err); !success {
		return err
	}
	// now we poll for completion
	for {
		time.Sleep(time.Minute)
		svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		params := &elasticsearchservice.DescribeElasticsearchDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		_, err := svc.DescribeElasticsearchDomain(params)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				// Instance no longer exists, this is success
				if awsErr.Code() == elasticsearchservice.ErrCodeResourceNotFoundException {
					d.logger.Debug("cleanupElasticSearchDomain - No ES Domain resource found, Deletion is a success")
					return nil
				}
				// Generic AWS error with Code, Message, and original error (if any)
				d.logger.Error("CleanUpESDomain - svc.DescribeElasticSearchDomain Failed", awsErr)
				//fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
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

	// assumerole to write to s3
	instanceCreds, err := i.getCredentials(password)
	if err != nil {
		d.logger.Error("writeManifesttoS3.getCredentials failed", err)
		return err
	}
	session, err := session.NewSession(&aws.Config{
		Region: aws.String(d.settings.Region),
		Credentials: credentials.NewStaticCredentials(
			instanceCreds["access_key"],
			instanceCreds["scret_key"],
			"",
		),
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
	d.logger.Debug(input.GoString())
	resp, err := svc.PutObject(&input)
	// Decide if AWS service call was successful
	d.logger.Debug(resp.GoString())
	if success := d.didAwsCallSucceed(err); !success {
		d.logger.Error("writeManifesttoS3.PutObject Failed", err)
		return err
	}
	return nil
}
