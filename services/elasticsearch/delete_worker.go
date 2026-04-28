package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"

	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	brokerAws "github.com/cloud-gov/aws-broker/aws"
	"github.com/cloud-gov/aws-broker/awsiam"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

const (
	DeleteKind = "elasticsearch-delete"
)

type DeleteArgs struct {
	Instance *ElasticsearchInstance `json:"instance"`
}

func (DeleteArgs) Kind() string { return DeleteKind }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	db         *gorm.DB
	settings   *config.Settings
	opensearch OpensearchClientInterface
	iam        awsiam.IAMClientInterface
	s3         brokerAws.S3ClientInterface
	logger     *slog.Logger
}

func NewDeleteWorker(
	db *gorm.DB,
	settings *config.Settings,
	opensearch OpensearchClientInterface,
	iam awsiam.IAMClientInterface,
	s3 brokerAws.S3ClientInterface,
	logger *slog.Logger,
) *DeleteWorker {
	return &DeleteWorker{
		db:         db,
		settings:   settings,
		opensearch: opensearch,
		iam:        iam,
		s3:         s3,
		logger:     logger,
	}
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return w.asyncDeleteElasticSearchDomain(ctx, job.Args.Instance)
}

func (w *DeleteWorker) asyncDeleteElasticSearchDomain(ctx context.Context, i *ElasticsearchInstance) error {
	operation := base.DeleteOp

	err := w.takeLastSnapshot(ctx, i)
	if err != nil {
		errorMsg := "asyncDeleteElasticSearchDomain - \t takeLastSnapshot returned error"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	err = w.writeManifestToS3(ctx, i)
	if err != nil {
		errorMsg := "asyncDeleteElasticSearchDomain - \t writeManifestToS3 returned error"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	err = w.cleanupRolesAndPolicies(ctx, i)
	if err != nil {
		errorMsg := "asyncDeleteElasticSearchDomain - \t cleanupRolesAndPolicies returned error"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	err = w.cleanupElasticSearchDomain(ctx, i)
	if err != nil {
		errorMsg := "asyncDeleteElasticSearchDomain - \t cleanupElasticSearchDomain returned error"
		w.logger.Error(errorMsg, "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, fmt.Sprintf("%s: %s ", errorMsg, err))
		return river.JobCancel(fmt.Errorf("%s: %w ", errorMsg, err))
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted resources")
	return nil
}

// in which we make the ES API call to take a snapshot
// then poll for snapshot completetion, may block for a considerable time
func (w *DeleteWorker) takeLastSnapshot(ctx context.Context, i *ElasticsearchInstance) error {
	var creds map[string]string
	var err error

	// check if instance was never bound and thus never set host...
	if i.Host == "" {
		creds, err = bindElasticsearchToApp(ctx, w.opensearch, w.iam, w.settings, w.logger, i)
		if err != nil {
			w.logger.Error("takeLastSnapshot: bindElasticsearchToApp failed", "err", err)
			return err
		}
	} else {
		creds, err = i.getCredentials()
		if err != nil {
			w.logger.Error("takeLastSnapshot: getCredentials failed", "err", err)
			return err
		}
	}

	// add broker snapshot bucket and create roles and policies if it hasnt been done.
	if !i.BrokerSnapshotsEnabled {
		if i.SnapshotPath == "" {
			i.SnapshotPath = "/" + i.OrganizationGUID + "/" + i.SpaceGUID + "/" + i.ServiceID + "/" + i.Uuid
		}
		iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)
		err := createUpdateBucketRolesAndPolicies(ctx, w.iam, w.logger, i, w.settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
		if err != nil {
			w.logger.Error("bindElasticsearchToApp - Error in createUpdateRolesAndPolicies", "err", err)
			return err
		}
		i.BrokerSnapshotsEnabled = true
	}

	// EsApiHandler takes care of v4 signing of requests, and other header/ request formation.
	esApi, err := NewEsApiHandler(ctx, creds, w.settings.Region, w.logger)
	if err != nil {
		w.logger.Error("NewEsApiHandler: %s", "err", err)
		return err
	}

	// create snapshot repo
	_, err = esApi.CreateSnapshotRepo(
		w.settings.SnapshotsRepoName,
		w.settings.SnapshotsBucketName,
		i.SnapshotPath,
		w.settings.Region,
		i.SnapshotARN,
	)
	if err != nil {
		w.logger.Error("createsnapshotrepo returns error", "err", err)
		return err
	}

	snapshotName := fmt.Sprintf("%s-%d", w.settings.LastSnapshotName, time.Now().Unix())

	// create snapshot
	_, err = esApi.CreateSnapshot(w.settings.SnapshotsRepoName, snapshotName)
	if err != nil {
		w.logger.Error("CreateSnapshot returns error", "err", err)
		return err
	}

	return w.pollForSnapshotCreation(esApi, snapshotName)
}

func (w *DeleteWorker) pollForSnapshotCreation(esApi EsApiClient, snapshotName string) error {
	var snapshotState string
	var err error
	attempts := 1

	for attempts <= int(w.settings.PollAwsMaxRetries) {
		snapshotState, err = esApi.GetSnapshotStatus(w.settings.SnapshotsRepoName, snapshotName)
		if err != nil {
			w.logger.Error("GetSnapShotStatus failed", "err", err)
			return err
		}
		if snapshotState == "SUCCESS" {
			break
		}
		attempts += 1
		time.Sleep(w.settings.PollAwsMinDelay)
	}

	if snapshotState != "SUCCESS" {
		return errors.New("Could not verify creation of snapshot")
	}

	return nil
}

// in which we clean up all the roles and policies for the ES domain
func (w *DeleteWorker) cleanupRolesAndPolicies(ctx context.Context, i *ElasticsearchInstance) error {
	if _, err := w.iam.DetachUserPolicy(ctx, &iam.DetachUserPolicyInput{
		PolicyArn: aws.String(i.IamPolicyARN),
		UserName:  aws.String(i.Domain),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DetachUserPolicy for IAM policy failed", "err", err)
		return err
	}

	if _, err := w.iam.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
		UserName:    aws.String(i.Domain),
		AccessKeyId: aws.String(i.AccessKey),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DeleteAccessKey failed", "err", err)
		return err
	}

	if _, err := w.iam.DetachUserPolicy(ctx, &iam.DetachUserPolicyInput{
		PolicyArn: aws.String(i.IamPassRolePolicyARN),
		UserName:  aws.String(i.Domain),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DetachUserPolicy for IAM pass role policy failed", "err", err)
		return err
	}

	if _, err := w.iam.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
		PolicyArn: aws.String(i.SnapshotPolicyARN),
		RoleName:  aws.String(i.Domain + "-to-s3-SnapshotRole"),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DetachRolePolicy failed", "err", err)
		return err
	}

	if _, err := w.iam.DeletePolicy(ctx, &iam.DeletePolicyInput{
		PolicyArn: aws.String(i.SnapshotPolicyARN),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DeletePolicy for IAM snapshot policy failed", "err", err)
		return err
	}

	if _, err := w.iam.DeleteRole(ctx, &iam.DeleteRoleInput{
		RoleName: aws.String(i.Domain + "-to-s3-SnapshotRole"),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DeleteRole failed", "err", err)
		return err
	}

	if _, err := w.iam.DeletePolicy(ctx, &iam.DeletePolicyInput{
		PolicyArn: aws.String(i.IamPassRolePolicyARN),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DeletePolicy for IAM pass role failed", "err", err)
		return err
	}

	deleteUserInput := &iam.DeleteUserInput{
		UserName: aws.String(i.Domain),
	}
	if _, err := w.iam.DeleteUser(ctx, deleteUserInput); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: user.Delete failed", "err", err)
		return err
	}

	if _, err := w.iam.DeletePolicy(ctx, &iam.DeletePolicyInput{
		PolicyArn: aws.String(i.IamPolicyARN),
	}); err != nil {
		w.logger.Error("cleanupRolesAndPolicies: DeletePolicy for IAM policy failed", "err", err)
		return err
	}
	return nil
}

// in which we finally delete the ES Domain and wait for it to complete
func (w *DeleteWorker) cleanupElasticSearchDomain(ctx context.Context, i *ElasticsearchInstance) error {
	params := &opensearch.DeleteDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err := w.opensearch.DeleteDomain(ctx, params)
	if err != nil {
		return err
	}

	// now we poll for completion
	attempts := 1

	for attempts <= int(w.settings.PollAwsMaxRetries) {
		params := &opensearch.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		_, err := w.opensearch.DescribeDomain(ctx, params)
		if err != nil {
			var notFoundException *opensearchTypes.ResourceNotFoundException
			if errors.As(err, &notFoundException) {
				// Instance no longer exists, this is success
				w.logger.Info(fmt.Sprintf("%s domain has been deleted", i.Domain))
				return nil
			}

			w.logger.Error("cleanupElasticSearchDomain: DescribeDomain err", "err", err)
			return err
		}

		attempts += 1
		time.Sleep(w.settings.PollAwsMinDelay)
	}

	return errors.New("could not verify deletion of domain")
}

// in which we Marshall the instance into Json and dump to a manifest file in the snapshot bucket
// so to provide machine readable information for restoration.
func (w *DeleteWorker) writeManifestToS3(ctx context.Context, i *ElasticsearchInstance) error {
	//  marshall instance to bytes.
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}
	body := bytes.NewReader(data)

	serverSideEncryption, err := brokerAws.GetS3ServerSideEncryptionEnum("AES256")
	if err != nil {
		return err
	}

	// put json blob into object in s3
	input := s3.PutObjectInput{
		Body:                 body,
		Bucket:               aws.String(w.settings.SnapshotsBucketName),
		Key:                  aws.String(i.SnapshotPath + "/instance_manifest.json"),
		ServerSideEncryption: *serverSideEncryption,
	}

	_, err = w.s3.PutObject(ctx, &input)
	if err != nil {
		w.logger.Error("writeManifesttoS3: PutObject err", "err", err)
		return err
	}

	return nil
}
