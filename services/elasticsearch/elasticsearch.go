package elasticsearch

import (
	"errors"
	"log"
	"os"
	"time"

	"code.cloudfoundry.org/lager"
	"github.com/18F/aws-broker/base"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/cloudfoundry-community/s3-broker/awsiam"
	"github.com/jinzhu/gorm"

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
	deleteElasticsearch(i *ElasticsearchInstance, passoword string) (base.InstanceState, error)
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

func (d *mockElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	// TODO
	return base.InstanceGone, nil
}

type sharedElasticsearchAdapter struct {
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
}

type dedicatedElasticsearchAdapter struct {
	Plan     catalog.ElasticsearchPlan
	settings config.Settings
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
	if i.BrokerSnapshotBucket != "" && !i.BrokerSnapshotsEnabled {
		// specify a path for the bucket access policy to scope to this instance
		// TODO: instead of ServiceId should we use domain?
		path := "/" + i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID
		err := d.createUpdateBucketRolesAndPolicies(i, i.BrokerSnapshotBucket, path)
		if err != nil {
			return nil, err
		}
		err = d.createSnapshotRepo(i, password, i.BrokerSnapshotBucket, path, d.settings.Region)
		if err != nil {
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

// we make the deletion async and return a 202
func (d *dedicatedElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	go d.asyncDeleteElasticSearchDomain(i, password)
	return base.InstanceInProgress, nil
}

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

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) == true {
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
	if i.State != base.InstanceReady {
		return errors.New("instance is not ready, cannont execute api calls")
	}
	creds, err := i.getCredentials(password)
	if err != nil {
		fmt.Println(err)
		return err
	}
	// EsApiHandler takes care of v4 signing of requests, and other header/ request formation.
	esApi := &EsApiHandler{}
	esApi.Init(creds, region)

	// create snapshot repo
	_, err = esApi.CreateSnapshotRepo(d.settings.SnapshotsRepoName, bucket, path, region, i.SnapshotARN)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// utility to create roles and policies to enable snapshots in an s3 bucket
// we pass bucket-name separately to enable reuse for client and broker buckets
func (d *dedicatedElasticsearchAdapter) createUpdateBucketRolesAndPolicies(i *ElasticsearchInstance, bucket string, path string) error {
	ip := iampolicy.NewIamPolicyHandler(d.settings.Region)
	var snapshotRole *iam.Role

	// create snapshotrole if not done yet
	if i.SnapshotARN == "" {
		rolename := i.Domain + "-to-s3-SnapshotRole"
		policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		arole, err := ip.CreateAssumeRole(policy, rolename)
		if err != nil {
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
			return err
		}
		policyarn, err := ip.CreatePolicyAttachRole(policyname, policy, *snapshotRole)
		if err != nil {
			return err
		}
		i.SnapshotPolicyARN = policyarn

	} else {
		//snaphostpolicy has already be created so we need to add the new statements for this new bucket
		//to the existing policy version.
		_, err := ip.UpdateExistingPolicy(i.SnapshotPolicyARN, []iampolicy.PolicyStatementEntry{listStatement, objectStatement})
		if err != nil {
			return err
		}

	}
	return nil
}

func (d *dedicatedElasticsearchAdapter) asyncDeleteElasticSearchDomain(i *ElasticsearchInstance, password string) {
	err := d.takeLastSnapshot(i, password)
	if err != nil {
		fmt.Println(err.Error())
		i.State = base.InstanceNotGone
		return
	}
	err = d.cleanupRolesAndPolicies(i)
	if err != nil {
		fmt.Println(err.Error())
		i.State = base.InstanceNotGone
		return
	}
	err = d.cleanupElasticSearchDomain(i)
	if err != nil {
		fmt.Println(err.Error())
		i.State = base.InstanceNotGone
		return
	}
	i.State = base.InstanceGone
}

// in which we make the ES API call to take a snapshot and poll for completion
// then poll for snapshot completetion, may block for a considerable time
func (d *dedicatedElasticsearchAdapter) takeLastSnapshot(i *ElasticsearchInstance, password string) error {
	// catch legacy domains that dont have snapshotbucket configured
	if i.BrokerSnapshotBucket == "" {
		i.BrokerSnapshotBucket = d.settings.SnapshotsBucketName
		path := "/" + i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID
		err := d.createUpdateBucketRolesAndPolicies(i, i.BrokerSnapshotBucket, path)
		if err != nil {
			return err
		}
		err = d.createSnapshotRepo(i, password, i.BrokerSnapshotBucket, path, d.settings.Region)
		if err != nil {
			return err
		}
		i.BrokerSnapshotsEnabled = true
	}
	// exec snapshot request

	// poll for snapshot completion

	return nil
}

// in which we finally delete the ES Domain
func (d *dedicatedElasticsearchAdapter) cleanupElasticSearchDomain(i *ElasticsearchInstance) error {
	svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	params := &elasticsearchservice.DeleteElasticsearchDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	resp, err := svc.DeleteElasticsearchDomain(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return nil
	}
	return err
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

	if i.BrokerSnapshotBucket != "" {
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
