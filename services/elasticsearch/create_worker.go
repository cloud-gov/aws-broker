package elasticsearch

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/awsiam"
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
	// IAM User and policy before domain starts creating so it can be used to create access control policy
	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
	_, err := w.iam.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(i.Domain),
		Path:     nil,
		Tags:     iamTags,
	})
	if err != nil {
		w.logger.Error("createElasticsearch: user.Create err", "err", err)
		return err
	}

	createAccessKeyOutput, err := w.iam.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: aws.String(i.Domain),
	})
	if err != nil {
		return err
	}
	i.AccessKey = *createAccessKeyOutput.AccessKey.AccessKeyId
	i.SecretKey = *createAccessKeyOutput.AccessKey.SecretAccessKey

	userParams := &iam.GetUserInput{
		UserName: aws.String(i.Domain),
	}
	userResp, err := w.iam.GetUser(ctx, userParams)
	if err != nil {
		w.logger.Error("createElasticsearch: GetUser err", "err", err)
		return err
	}
	uniqueUserArn := *(userResp.User.Arn)
	i.IamUserARN = uniqueUserArn

	stsInput := &sts.GetCallerIdentityInput{}
	result, err := w.sts.GetCallerIdentity(ctx, stsInput)
	if err != nil {
		w.logger.Error("createElasticsearch: GetCallerIdentity err", "err", err)
		return err
	}

	accountID := result.Account

	// Set up cloudwatch log groups
	if err := setupLogging(ctx, i, w.logs, w.logger, w.settings, *accountID); err != nil {
		w.logger.Error("createElasticsearch: setupLogging err", "err", err)
		return err
	}

	time.Sleep(5 * time.Second)

	accessControlPolicy := "{\"Version\": \"2012-10-17\",\"Statement\": [{\"Effect\": \"Allow\",\"Principal\": {\"AWS\": \"" + uniqueUserArn + "\"},\"Action\": \"es:*\",\"Resource\": \"arn:aws-us-gov:es:" + w.settings.Region + ":" + *accountID + ":domain/" + i.Domain + "/*\"}]}"
	params, err := prepareCreateDomainInput(i, accessControlPolicy)
	if err != nil {
		w.logger.Error("createElasticsearch: prepareCreateDomainInput err", "err", err)
		return err
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
		w.logger.Error("createElasticsearch: CreateDomain err", "err", err)
		return err
	}

	i.ARN = *(resp.DomainStatus.ARN)
	esARNs := make([]string, 0)
	esARNs = append(esARNs, i.ARN)
	policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
	policyARN, err := awsiam.CreatePolicyFromTemplate(ctx, w.iam, w.logger, i.Domain, "/", policy, esARNs, iamTags)
	if err != nil {
		return err
	}

	if _, err = w.iam.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{
		PolicyArn: aws.String(policyARN),
		UserName:  aws.String(i.Domain),
	}); err != nil {
		return err
	}
	i.IamPolicy = policy
	i.IamPolicyARN = policyARN

	//try setup of roles and policies on create
	err = createUpdateBucketRolesAndPolicies(ctx, w.iam, w.logger, i, w.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
	if err != nil {
		return err
	}

	i.BrokerSnapshotsEnabled = true
	return nil
}
