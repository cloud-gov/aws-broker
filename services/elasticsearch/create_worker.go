package elasticsearch

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/awsiam"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	CreateKind = "elasticsearch-create"
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
	return w.createDomain(ctx, job.Args.Instance)
}

func (w *CreateWorker) createDomain(ctx context.Context, i *ElasticsearchInstance) error {
	operation := base.CreateOp

	// IAM User and policy before domain starts creating so it can be used to create access control policy
	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
	_, err := w.iam.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(i.Domain),
		Path:     nil,
		Tags:     iamTags,
	})
	if err != nil {
		errorMsg := "createElasticsearch: user.Create err"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	createAccessKeyOutput, err := w.iam.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: aws.String(i.Domain),
	})
	if err != nil {
		errorMsg := "error creating access keys"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}
	i.AccessKey = *createAccessKeyOutput.AccessKey.AccessKeyId
	i.SecretKey = *createAccessKeyOutput.AccessKey.SecretAccessKey

	userParams := &iam.GetUserInput{
		UserName: aws.String(i.Domain),
	}
	userResp, err := w.iam.GetUser(ctx, userParams)
	if err != nil {
		errorMsg := "createElasticsearch: GetUser err"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}
	uniqueUserArn := *(userResp.User.Arn)
	i.IamUserARN = uniqueUserArn

	stsInput := &sts.GetCallerIdentityInput{}
	result, err := w.sts.GetCallerIdentity(ctx, stsInput)
	if err != nil {
		errorMsg := "createElasticsearch: GetCallerIdentity err"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	accountID := result.Account

	// Set up cloudwatch log groups
	if err := setupLogging(ctx, i, w.logs, w.logger, w.settings, *accountID); err != nil {
		errorMsg := "createElasticsearch: setupLogging err"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	time.Sleep(5 * time.Second)

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"" + uniqueUserArn + "\"},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + w.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	params, err := prepareCreateDomainInput(i, accessControlPolicy)
	if err != nil {
		errorMsg := "createElasticsearch: prepareCreateDomainInput err"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	resp, err := w.opensearch.CreateDomain(ctx, params)
	if isInvalidTypeException(err) {
		// IAM is eventually consistent, meaning new IAM users may not be immediately available for read, such as when
		// Opensearch goes to validate the IAM user specified as the AWS principal in the access
		// policy. The error returned in this case is an "InvalidTypeException", so if we catch that specific error,
		// we wait for 5 seconds to retry the domain creation to hopefully allow IAM to become consistent.
		//
		// see https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency
		log.Println("Retrying domain creation because of possible IAM eventual consistency issue")
		time.Sleep(5 * time.Second)
		resp, err = w.opensearch.CreateDomain(ctx, params)
	}

	// Decide if AWS service call was successful
	if err != nil {
		errorMsg := "createElasticsearch: CreateDomain err"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	i.ARN = *(resp.DomainStatus.ARN)
	esARNs := make([]string, 0)
	esARNs = append(esARNs, i.ARN)
	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
	policyARN, err := awsiam.CreatePolicyFromTemplate(ctx, w.iam, w.logger, i.Domain, "/", policy, esARNs, iamTags)
	if err != nil {
		errorMsg := "error creating IAM policy"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	if _, err = w.iam.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(i.Domain),
	}); err != nil {
		errorMsg := "error attaching IAM user policy"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}
	i.IamPolicy = policy
	i.IamPolicyARN = policyARN

	//try setup of roles and policies on create
	err = createUpdateBucketRolesAndPolicies(ctx, w.iam, w.logger, i, w.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
	if err != nil {
		errorMsg := "error setting up snapshot bucket roles and policies"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	i.BrokerSnapshotsEnabled = true
	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceReady, "Finished creating domain")
	return nil
}
