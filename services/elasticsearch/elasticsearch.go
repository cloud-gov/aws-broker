package elasticsearch

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/cloud-gov/aws-broker/asyncmessage"
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
	checkCompatibleVersions(domainName, targetVersion string) error
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

func (d *mockElasticsearchAdapter) checkCompatibleVersions(domainName, targetVersion string) error {
	return nil
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
		s3:          s3.NewFromConfig(cfg),
		logs:        cloudwatchlogs.NewFromConfig(cfg),
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
	s3          brokerAws.S3ClientInterface
	logs        CloudwatchLogsClientInterface
	riverClient *river.Client[*sql.Tx]
}

// This is the prefix for all pgroups created by the broker.
const PgroupPrefix = "cg-elasticsearch-broker-"

func (d *dedicatedElasticsearchAdapter) createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	err := asyncmessage.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.CreateOp, base.InstanceInProgress, "Creating domain")
	if err != nil {
		return base.InstanceNotCreated, err
	}

	tx := d.db.Begin()
	if err := tx.Error; err != nil {
		return base.InstanceNotCreated, err
	}
	defer tx.Rollback()

	sqlTx := tx.Statement.ConnPool.(*sql.Tx)

	_, err = d.riverClient.InsertTx(d.ctx, sqlTx, &CreateArgs{
		Instance: i,
	}, nil)
	if err != nil {
		return base.InstanceNotCreated, err
	}

	if err := tx.Commit().Error; err != nil {
		return base.InstanceNotCreated, err
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
	if i.versionUpgradeInProgress() {
		_, err := d.opensearch.UpgradeDomain(d.ctx, &opensearch.UpgradeDomainInput{
			DomainName:    aws.String(i.Domain),
			TargetVersion: aws.String(i.TargetElasticsearchVersion),
		})
		if err != nil {
			d.logger.Error("modifyElasticsearch: UpgradeDomain err", "err", err)
			return base.InstanceNotModified, err
		}
		return base.InstanceInProgress, nil
	}

	// Ensure any newly request log groups exist and resolve the IAM master user ARN before referencing
	// in the domain config update
	if err := d.ensureLoggingForModify(i); err != nil {
		d.logger.Error("modifyElasticsearch: ensureLoggingForModify err", "err", err)
		return base.InstanceNotModified, err
	}

	params, err := prepareUpdateDomainConfigInput(i)
	if err != nil {
		return base.InstanceNotModified, err
	}

	_, err = d.opensearch.UpdateDomainConfig(d.ctx, params)
	if err != nil {
		d.logger.Error("modifyElasticsearch: UpdateDomainConfig err", "err", err)
		return base.InstanceNotModified, err
	}

	return base.InstanceInProgress, nil
}

func (d *dedicatedElasticsearchAdapter) ensureLoggingForModify(i *ElasticsearchInstance) error {
	if !i.anyLogsEnabled() && !i.AdvancedSecurityEnabled {
		return nil
	}

	result, err := d.sts.GetCallerIdentity(d.ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return err
	}

	// FGAC needs IAM user ARN as master user. Look it up if not already persisted to the instance.
	if i.AdvancedSecurityEnabled && i.IamUserARN == "" {
		userResp, err := d.iam.GetUser(d.ctx, &iam.GetUserInput{UserName: aws.String(i.Domain)})
		if err != nil {
			return err
		}
		i.IamUserARN = *userResp.User.Arn
	}

	return setupLogging(d.ctx, i, d.logs, d.logger, &d.settings, *result.Account)
}

func (d *dedicatedElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	return bindElasticsearchToApp(d.ctx, d.opensearch, d.iam, &d.settings, d.logger, i)
}

// we make the deletion async, set status to in-progress and rollup to return a 202
func (d *dedicatedElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	err := asyncmessage.WriteAsyncJobMessage(d.db, i.ServiceID, i.Uuid, base.DeleteOp, base.InstanceInProgress, "Deleting resources")
	if err != nil {
		return base.InstanceNotGone, err
	}

	//check for backing resource and do async otherwise remove from db
	params := &opensearch.DescribeDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	_, err = d.opensearch.DescribeDomain(d.ctx, params)
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

		resp, err := d.opensearch.DescribeDomain(d.ctx, params)
		if err != nil {
			d.logger.Error("checkElasticsearchStatus: UpdateDomainConfig err", "err", err)
			return base.InstanceNotCreated, err
		}

		d.logger.Debug(fmt.Sprintf("domain status: %+v\n", resp.DomainStatus))

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			if i.versionUpgradeInProgress() {
				if aws.ToBool(resp.DomainStatus.UpgradeProcessing) {
					return base.InstanceInProgress, nil
				}
				if aws.ToString(resp.DomainStatus.EngineVersion) == i.TargetElasticsearchVersion {
					i.ElasticsearchVersion = i.TargetElasticsearchVersion
					i.TargetElasticsearchVersion = ""
					return base.InstanceReady, nil
				}
				d.logger.Error(
					"checkElasticsearchStatus: version upgrade did not complete",
					"domain", i.Domain,
					"engineVersion", aws.ToString(resp.DomainStatus.EngineVersion),
					"targetVersion", i.TargetElasticsearchVersion,
				)
				i.TargetElasticsearchVersion = ""
				return base.InstanceNotModified, nil
			}

			if aws.ToBool(resp.DomainStatus.Processing) {
				return base.InstanceInProgress, nil
			}

			return base.InstanceReady, nil
		} else {
			// Instance not up yet.
			return base.InstanceNotCreated, errors.New("instance not available yet. Please wait and try again")
		}
	}
	return base.InstanceNotCreated, nil

}

func (d *dedicatedElasticsearchAdapter) checkCompatibleVersions(domainName, targetVersion string) error {
	resp, err := d.opensearch.GetCompatibleVersions(d.ctx, &opensearch.GetCompatibleVersionsInput{
		DomainName: aws.String(domainName),
	})
	if err != nil {
		return fmt.Errorf("checking compatible versions: %w", err)
	}

	// CompatibleVersions is a graph of available upgrade paths.
	// When called with a domain name, AWS returns a single entry of the available upgrade paths from the domain's current version.
	if len(resp.CompatibleVersions) == 0 {
		return fmt.Errorf("%s is not a valid upgrade target; no upgrade paths are available from the current version", targetVersion)
	}

	entry := resp.CompatibleVersions[0]
	sourceVersion := aws.ToString(entry.SourceVersion)

	if slices.Contains(entry.TargetVersions, targetVersion) {
		return nil
	}

	if len(entry.TargetVersions) == 0 {
		return fmt.Errorf("%s is not a valid upgrade target; no upgrade paths are available from %s", targetVersion, sourceVersion)
	}

	return fmt.Errorf("%s is not a valid upgrade target from %s; compatible versions are: %s", targetVersion, sourceVersion, strings.Join(entry.TargetVersions, ", "))

}

// determine whether the error is an opensearch.InvalidTypeException
func isInvalidTypeException(createErr error) bool {
	var InvalidTypeException *opensearchTypes.InvalidTypeException
	return errors.As(createErr, &InvalidTypeException)
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

		volumeSize, err := common.ConvertIntToInt32Safely(i.VolumeSize)
		if err != nil {
			return nil, err
		}

		params.EBSOptions = &opensearchTypes.EBSOptions{
			EBSEnabled: aws.Bool(true),
			VolumeSize: aws.Int32(*volumeSize),
			VolumeType: *volumeType,
		}
	}

	if logPublishingOptions := buildLogPublishingOptions(i); len(logPublishingOptions) > 0 {
		params.LogPublishingOptions = logPublishingOptions
	}

	if !i.AuditRestConfigApplied {
		advancedSecurityOptions, err := advancedSecurityOptionsForAudit(i)
		if err != nil {
			return nil, err
		}
		if advancedSecurityOptions != nil {
			params.AdvancedSecurityOptions = advancedSecurityOptions
		}
	}

	return params, nil
}
