package elasticsearch

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"

	"github.com/aws/aws-sdk-go-v2/service/iam"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/awsiam"
	"github.com/cloud-gov/aws-broker/base"

	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"

	"fmt"
)

type ElasticsearchAdapter interface {
	createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error)
	checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error)
	bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error)
	deleteElasticsearch(i *ElasticsearchInstance, passoword string) (base.InstanceState, error)
}

type mockElasticsearchAdapter struct {
}

func (d *mockElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

func (d *mockElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

func (d *mockElasticsearchAdapter) checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error) {
	return base.InstanceReady, nil
}

func (d *mockElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	return i.getCredentials()
}

func (d *mockElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	return base.InstanceInProgress, nil
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(ctx context.Context, db *gorm.DB, s *config.Settings, logger *slog.Logger, riverClient *river.Client[*sql.Tx]) (ElasticsearchAdapter, error) {
	var elasticsearchAdapter ElasticsearchAdapter

	if s.Environment == "test" {
		elasticsearchAdapter = &mockElasticsearchAdapter{}
		return elasticsearchAdapter, nil
	}

	cfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithRegion(s.Region),
	)
	if err != nil {
		return nil, err
	}

	iamSvc := iam.NewFromConfig(cfg)

	elasticsearchAdapter = &dedicatedElasticsearchAdapter{
		ctx:         ctx,
		db:          db,
		settings:    *s,
		logger:      logger,
		opensearch:  opensearch.NewFromConfig(cfg),
		iam:         iamSvc,
		sts:         sts.NewFromConfig(cfg),
		ip:          awsiam.NewIAMPolicyClient(iamSvc, logger),
		s3:          s3.NewFromConfig(cfg),
		riverClient: riverClient,
	}

	return elasticsearchAdapter, nil
}

type dedicatedElasticsearchAdapter struct {
	ctx         context.Context
	db          *gorm.DB
	settings    config.Settings
	logger      *slog.Logger
	iam         awsiam.IAMClientInterface
	sts         STSClientInterface
	opensearch  OpensearchClientInterface
	ip          *awsiam.IAMPolicyClient
	s3          brokerAws.S3ClientInterface
	riverClient *river.Client[*sql.Tx]
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-elasticsearch-broker-"

func (d *dedicatedElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	user := awsiam.NewIAMUserClient(d.iam, d.logger)

	// IAM User and policy before domain starts creating so it can be used to create access control policy
	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
	_, err := user.Create(i.Domain, "", iamTags)
	if err != nil {
		d.logger.Error("createElasticsearch: user.Create err", "err", err)
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
		d.logger.Error("createElasticsearch: GetCallerIdentity err", "err", err)
		return base.InstanceNotCreated, nil
	}

	accountID := result.Account

	time.Sleep(5 * time.Second)

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"" + uniqueUserArn + "\"},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + d.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	params, err := prepareCreateDomainInput(i, accessControlPolicy)
	if err != nil {
		d.logger.Error("createElasticsearch: prepareCreateDomainInput err", "err", err)
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
		d.logger.Error("createElasticsearch: CreateDomain err", "err", err)
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
	err = createUpdateBucketRolesAndPolicies(d.ip, d.logger, i, d.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
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
		d.logger.Error("modifyElasticsearch: UpdateDomainConfig err", "err", err)
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
			d.logger.Error("bindElasticsearchToApp: UpdateDomainConfig err", "err", err)
			return nil, err
		}

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			if resp.DomainStatus.Endpoints != nil && resp.DomainStatus.ARN != nil {
				d.logger.Debug(fmt.Sprintf("endpoint: %s ARN: %s \n", resp.DomainStatus.Endpoints["vpc"], *(resp.DomainStatus.ARN)))
				i.Host = resp.DomainStatus.Endpoints["vpc"]
				i.ARN = *(resp.DomainStatus.ARN)
				i.State = base.InstanceReady
				i.CurrentESVersion = *(resp.DomainStatus.EngineVersion)
				// Should only be one regardless. Just return now.
			} else {
				// Something went horribly wrong. Should never get here.
				return nil, errors.New("invalid memory for endpoint and/or endpoint members")
			}
		} else {
			// Instance not up yet.
			return nil, errors.New("instance not available yet. Please wait and try again")
		}

	}

	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)

	// add broker snapshot bucket and create roles and policies if it hasnt been done.
	if !i.BrokerSnapshotsEnabled {
		if i.SnapshotPath == "" {
			i.SnapshotPath = "/" + i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
		}

		err := createUpdateBucketRolesAndPolicies(d.ip, d.logger, i, d.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
		if err != nil {
			d.logger.Error("bindElasticsearchToApp - Error in createUpdateRolesAndPolicies", "err", err)
			return nil, err
		}
		i.BrokerSnapshotsEnabled = true
	}

	// add client bucket and adjust policies and roles if present
	if i.Bucket != "" {
		err := createUpdateBucketRolesAndPolicies(d.ip, d.logger, i, i.Bucket, "", iamTags)
		if err != nil {
			return nil, err
		}
	}
	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials()
}

// we make the deletion async, set status to in-progress and rollup to return a 202
func (d *dedicatedElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	//check for backing resource and do async otherwise remove from db
	params := &opensearch.DescribeDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err := d.opensearch.DescribeDomain(d.ctx, params)
	if err != nil {
		var notFoundException *opensearchTypes.ResourceNotFoundException
		if errors.As(err, &notFoundException) {
			return base.InstanceGone, err
		}

		d.logger.Error("deleteElasticsearch: DescribeDomain error", "err", err)
		return base.InstanceNotGone, err
	}

	tx := d.db.Begin()
	if err := tx.Error; err != nil {
		return base.InstanceNotGone, err
	}
	defer tx.Rollback()

	sqlTx := tx.Statement.ConnPool.(*sql.Tx)

	_, err = d.riverClient.InsertTx(d.ctx, sqlTx, &DeleteArgs{
		Instance: i,
	}, nil)
	if err != nil {
		return base.InstanceNotGone, err
	}

	if err := tx.Commit().Error; err != nil {
		return base.InstanceNotGone, err
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
			d.logger.Error("checkElasticsearchStatus: UpdateDomainConfig err", "err", err)
			return base.InstanceNotCreated, err
		}

		d.logger.Debug(fmt.Sprintf("domain status: %+v\n", resp.DomainStatus))

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
			return base.InstanceNotCreated, errors.New("instance not available yet. Please wait and try again")
		}
	}
	return base.InstanceNotCreated, nil

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

	volumeSize, err := common.ConvertIntToInt32Safely(i.VolumeSize)
	if err != nil {
		return nil, err
	}

	instanceCount, err := common.ConvertIntToInt32Safely(i.DataCount)
	if err != nil {
		return nil, err
	}

	ebsoptions := &opensearchTypes.EBSOptions{
		EBSEnabled: aws.Bool(true),
		VolumeSize: aws.Int32(*volumeSize),
		VolumeType: *volumeType,
	}

	esclusterconfig := &opensearchTypes.ClusterConfig{
		InstanceType:  *instanceType,
		InstanceCount: aws.Int32(*instanceCount),
	}

	if i.MasterEnabled {
		masterInstanceType, err := getOpensearchInstanceTypeEnum(i.MasterInstanceType)
		if err != nil {
			return nil, err
		}

		masterCount, err := common.ConvertIntToInt32Safely(i.MasterCount)
		if err != nil {
			return nil, err
		}

		esclusterconfig.DedicatedMasterEnabled = aws.Bool(i.MasterEnabled)
		esclusterconfig.DedicatedMasterCount = aws.Int32(*masterCount)
		esclusterconfig.DedicatedMasterType = *masterInstanceType
	}

	// Check AutomatedSnapshotStartHour is in valid range before casting.
	if i.AutomatedSnapshotStartHour < 0 || i.AutomatedSnapshotStartHour > 23 {
		return nil, fmt.Errorf("AutomatedSnapshotStartHour must be between 0 and 23, got %d", i.AutomatedSnapshotStartHour)
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
