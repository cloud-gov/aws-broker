package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/cloud-gov/aws-broker/awsiam"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
)

func createUpdateBucketRolesAndPolicies(
	ctx context.Context,
	iam awsiam.IAMClientInterface,
	logger *slog.Logger,
	i *ElasticsearchInstance,
	bucket string,
	path string,
	iamTags []iamTypes.Tag,
) error {
	var snapshotRole *iamTypes.Role

	// create snapshotrole if not done yet
	if i.SnapshotARN == "" {
		rolename := i.getSnapshotRoleName()
		policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		arole, err := awsiam.CreateAssumeRole(ctx, iam, logger, policy, rolename, iamTags)
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- CreateAssumeRole Error", "err", err)
			return err
		}

		i.SnapshotARN = *arole.Arn
		snapshotRole = arole

	}

	// create PassRolePolicy if DNE
	if i.IamPassRolePolicyARN == "" {
		passRoleStatement := awsiam.PolicyStatementEntry{
			Action:   []string{"iam:PassRole"},
			Effect:   "Allow",
			Resource: []string{i.SnapshotARN},
		}
		esHttpPutStatement := awsiam.PolicyStatementEntry{
			Action:   []string{"es:ESHttpPut"},
			Effect:   "Allow",
			Resource: []string{fmt.Sprintf("%s/*", i.SnapshotARN)},
		}
		policyDoc := awsiam.PolicyDocument{
			Version:   "2012-10-17",
			Statement: []awsiam.PolicyStatementEntry{passRoleStatement, esHttpPutStatement},
		}
		policy, err := policyDoc.ToString()
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- policyDoc.ToString Error", "err", err)
			return err
		}

		policyname := i.getPassRolePolicyName()
		username := i.Domain
		policyarn, err := awsiam.CreateUserPolicy(ctx, iam, logger, policy, policyname, username, iamTags)
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- CreateUserPolicy Error", "err", err)
			return err
		}
		i.IamPassRolePolicyARN = policyarn
	}

	// Create PolicyDoc Statements
	// looks like: {"Action": ["s3:ListBucket"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `"]}
	bucketArn := fmt.Sprintf("arn:aws-us-gov:s3:::%s", bucket)
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

		policyname := i.getSnapshotRolePolicyName()
		policy, err := policyDoc.ToString()
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- policyDoc.ToString Error", "err", err)
			return err
		}
		policyarn, err := awsiam.CreatePolicyAttachRole(ctx, iam, logger, policyname, policy, *snapshotRole, iamTags)
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- CreatePolicyAttachRole Error", "err", err)
			return err
		}
		i.SnapshotPolicyARN = policyarn

	} else {
		// snapshot policy has already been created so we need to add the new statements for this new bucket
		// to the existing policy version.
		_, err := awsiam.UpdateExistingPolicy(ctx, iam, logger, i.SnapshotPolicyARN, []awsiam.PolicyStatementEntry{listStatement, objectStatement})
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- UpdateExistingPolicy Error", "err", err)
			return err
		}

	}
	return nil
}

func bindElasticsearchToApp(ctx context.Context, opensearchClient OpensearchClientInterface, iam awsiam.IAMClientInterface, settings *config.Settings, logger *slog.Logger, i *ElasticsearchInstance) (map[string]string, error) {
	if !i.hasDomainProperties() {
		params := &opensearch.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := opensearchClient.DescribeDomain(ctx, params)
		if err != nil {
			logger.Error("bindElasticsearchToApp: UpdateDomainConfig err", "err", err)
			return nil, err
		}

		if resp.DomainStatus.Created == nil || (resp.DomainStatus.Created != nil && !*(resp.DomainStatus.Created)) {
			// Instance not up yet.
			return nil, errors.New("instance not available yet. Please wait and try again")
		}

		if resp.DomainStatus.Endpoints == nil && resp.DomainStatus.ARN == nil {
			// Something went horribly wrong. Should never get here.
			return nil, errors.New("invalid memory for endpoint and/or endpoint members")
		}

		i.setDomainProperties(resp.DomainStatus)
		i.State = base.InstanceReady
	}

	iamTags := awsiam.ConvertTagsMapToIAMTags(i.Tags)

	// add broker snapshot bucket and create roles and policies if it hasn't been done.
	if !i.brokerSnapshotsAreEnabled() {
		err := i.enableBrokerSnapshots(ctx, iam, settings, iamTags, logger)
		if err != nil {
			return nil, err
		}
	}

	// add client bucket and adjust policies and roles if present
	if i.Bucket != "" {
		err := createUpdateBucketRolesAndPolicies(ctx, iam, logger, i, i.Bucket, "", iamTags)
		if err != nil {
			return nil, err
		}
	}

	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(settings)
}

// setupLogging ensures the cloudwatch log groups for every enabled log type exists.
func setupLogging(
	ctx context.Context,
	i *ElasticsearchInstance,
	logs CloudwatchLogsClientInterface,
	logger *slog.Logger,
	settings *config.Settings,
	accountID string,
) error {
	if !i.anyLogsEnabled() {
		return nil
	}
	return ensureLogGroups(ctx, logs, logger, i, settings.OpensearchLogRetentionDays, settings.Region, accountID)
}
