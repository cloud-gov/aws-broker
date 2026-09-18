package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/awsiam"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	CreateKind = "elasticsearch-create"
)

var (
	ErrUpdatingInstance = errors.New("error saving updated instance")
)

type CreateArgs struct {
	Instance *ElasticsearchInstance `json:"instance"`
}

func (CreateArgs) Kind() string { return CreateKind }

type CreateWorker struct {
	river.WorkerDefaults[CreateArgs]
	db         *gorm.DB
	settings   *config.Settings
	opensearch OpensearchClientInterface
	iam        awsiam.IAMClientInterface
	s3         brokerAws.S3ClientInterface
	logs       CloudwatchLogsClientInterface
	sts        STSClientInterface
	logger     *slog.Logger
}

func NewCreateWorker(
	db *gorm.DB,
	settings *config.Settings,
	opensearch OpensearchClientInterface,
	iam awsiam.IAMClientInterface,
	s3 brokerAws.S3ClientInterface,
	logs CloudwatchLogsClientInterface,
	sts STSClientInterface,
	logger *slog.Logger,
) *CreateWorker {
	return &CreateWorker{
		db:         db,
		settings:   settings,
		opensearch: opensearch,
		iam:        iam,
		s3:         s3,
		logs:       logs,
		logger:     logger,
		sts:        sts,
	}
}

func (w *CreateWorker) Work(ctx context.Context, job *river.Job[CreateArgs]) error {
	operation := base.CreateOp
	i := job.Args.Instance
	err := w.createDomain(ctx, i, operation)
	if err != nil {
		w.logger.Error("error during domain creation", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, err.Error())
		return river.JobCancel(err)
	}
	return nil
}

func (w *CreateWorker) createDomain(ctx context.Context, i *ElasticsearchInstance, operation base.Operation) error {
	// IAM User and policy before domain starts creating so it can be used to create access control policy
	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
	resp, err := w.iam.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(i.getIamUsername()),
		Path:     nil,
		Tags:     iamTags,
	})
	if err != nil {
		return fmt.Errorf("error creating user: %w", err)
	}

	i.setIamUserARN(*resp.User.Arn)
	err = w.saveUpdatedInstance(i)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrUpdatingInstance, err)
	}

	createAccessKeyOutput, err := w.iam.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: aws.String(i.getIamUsername()),
	})
	if err != nil {
		return fmt.Errorf("error creating access keys: %w", err)
	}

	i.setAccessCredentials(*createAccessKeyOutput.AccessKey.AccessKeyId, *createAccessKeyOutput.AccessKey.SecretAccessKey)
	err = w.saveUpdatedInstance(i)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrUpdatingInstance, err)
	}

	stsInput := &sts.GetCallerIdentityInput{}
	result, err := w.sts.GetCallerIdentity(ctx, stsInput)
	if err != nil {
		return fmt.Errorf("error getting account information: %w", err)
	}

	accountID := result.Account

	// Set up cloudwatch log groups
	if err := setupLogging(ctx, i, w.logs, w.logger, w.settings, *accountID); err != nil {
		return fmt.Errorf("error setting up domain logging: %w", err)
	}

	time.Sleep(w.settings.PollAwsMinDelay)

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"" + i.getIamUserARN() + "\"},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + w.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	params, err := prepareCreateDomainInput(i, accessControlPolicy)
	if err != nil {
		return fmt.Errorf("error preparing domain creation input: %w", err)
	}

	_, err = w.opensearch.CreateDomain(ctx, params)
	if isInvalidTypeException(err) {
		// IAM is eventually consistent, meaning new IAM users may not be immediately available for read, such as when
		// Opensearch goes to validate the IAM user specified as the AWS principal in the access
		// policy. The error returned in this case is an "InvalidTypeException", so if we catch that specific error,
		// we wait for 5 seconds to retry the domain creation to hopefully allow IAM to become consistent.
		//
		// see https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency
		log.Println("Retrying domain creation because of possible IAM eventual consistency issue")
		time.Sleep(w.settings.PollAwsMinDelay)
		_, err = w.opensearch.CreateDomain(ctx, params)
	}

	if err != nil {
		return fmt.Errorf("error creating domain: %w", err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Waiting for domain to be ready")

	domainStatus, err := w.waitForDomainReady(ctx, i)
	if err != nil {
		return fmt.Errorf("error waiting for domain creation: %w", err)
	}

	// Audit logging requires a one-time REST call once the domain is ready
	if err := w.configureAuditLoggingIfNeeded(ctx, i, domainStatus); err != nil {
		return fmt.Errorf("error configuring audit logging: %w", err)
	}

	i.setDomainProperties(domainStatus)
	err = w.saveUpdatedInstance(i)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrUpdatingInstance, err)
	}

	esARNs := make([]string, 0)
	esARNs = append(esARNs, i.ARN)
	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
	policyARN, err := awsiam.CreatePolicyFromTemplate(ctx, w.iam, w.logger, i.Domain, "/", policy, esARNs, iamTags)
	if err != nil {
		return fmt.Errorf("error creating IAM policy: %w", err)
	}

	if _, err = w.iam.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(i.getIamUsername()),
	}); err != nil {
		return fmt.Errorf("error attaching IAM user policy: %w", err)
	}

	i.setUserIAMPolicyAttributes(policy, policyARN)
	err = w.saveUpdatedInstance(i)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrUpdatingInstance, err)
	}

	//try setup of roles and policies on create
	err = i.enableBrokerSnapshots(ctx, w.iam, w.settings, iamTags, w.logger)
	if err != nil {
		return fmt.Errorf("error setting up snapshot bucket roles and policies: %w", err)
	}

	i.State = base.InstanceReady
	err = w.saveUpdatedInstance(i)
	if err != nil {
		return fmt.Errorf("%s: %w", ErrUpdatingInstance, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished creating domain")
	return nil
}

func (w *CreateWorker) saveUpdatedInstance(i *ElasticsearchInstance) error {
	return w.db.Save(i).Error
}

func (w *CreateWorker) configureAuditLoggingIfNeeded(
	ctx context.Context,
	i *ElasticsearchInstance,
	domainStatus *opensearchTypes.DomainStatus,
) error {
	if !i.AuditLogsEnabled || i.AuditRestConfigApplied {
		return nil
	}

	endpoint := domainStatus.Endpoints["vpc"]
	if endpoint == "" {
		return errors.New("domain endpoint not available yet")
	}

	creds, err := i.getCredentials()
	if err != nil {
		return err
	}

	esApi, err := NewEsApiHandler(ctx, creds, w.settings.Region, w.logger)
	if err != nil {
		return err
	}

	// Use the engine version reported bythe domain to pick the correct security API path
	engineVersion := aws.ToString(domainStatus.EngineVersion)
	if err := esApi.EnableAuditLogging(engineVersion); err != nil {
		return err
	}

	i.AuditRestConfigApplied = true
	return nil
}

func (w *CreateWorker) waitForDomainReady(
	ctx context.Context,
	i *ElasticsearchInstance,
) (*opensearchTypes.DomainStatus, error) {
	w.logger.Debug(fmt.Sprintf("Waiting for domain %s to be available", i.Domain))

	var resp *opensearch.DescribeDomainOutput
	var err error
	attempts := 1

	for attempts <= getPollAwsMaxRetries(int(w.settings.PollAwsMaxRetries)) {
		resp, err = w.opensearch.DescribeDomain(ctx, &opensearch.DescribeDomainInput{
			DomainName: &i.Domain,
		})
		if err != nil {
			w.logger.Error("describe domain failed", "err", err)
			return nil, err
		}
		if isDomainReady(resp) {
			break
		}
		attempts += 1
		time.Sleep(w.settings.PollAwsMinDelay)
	}

	if !isDomainReady(resp) {
		return nil, errors.New("could not verify creation of domain")
	}

	return resp.DomainStatus, nil
}

func getPollAwsMaxRetries(defaultMaxRetries int) int {
	// give more retries waiting for domain operations, which can be
	// very time consuming
	return 2 * defaultMaxRetries
}

func isDomainReady(output *opensearch.DescribeDomainOutput) bool {
	return output.DomainStatus.Created != nil && *(output.DomainStatus.Created) &&
		output.DomainStatus.Endpoints != nil && output.DomainStatus.Endpoints["vpc"] != ""
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

	if logPublishingOptions := buildLogPublishingOptions(i); len(logPublishingOptions) > 0 {
		params.LogPublishingOptions = logPublishingOptions
	}

	advancedSecurityOptions, err := advancedSecurityOptionsForAudit(i)
	if err != nil {
		return nil, err
	}
	if advancedSecurityOptions != nil {
		params.AdvancedSecurityOptions = advancedSecurityOptions
	}

	if i.ElasticsearchVersion != "" {
		params.EngineVersion = aws.String(i.ElasticsearchVersion)
	}

	return params, nil
}
