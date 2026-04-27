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
	ip *awsiam.IAMPolicyClient,
	logger *slog.Logger,
	i *ElasticsearchInstance,
	bucket string,
	path string,
	iamTags []iamTypes.Tag,
) error {
	// ip := awsiam.NewIAMPolicyClient(settings.Region, logger)
	var snapshotRole *iamTypes.Role

	// create snapshotrole if not done yet
	if i.SnapshotARN == "" {
		rolename := i.Domain + "-to-s3-SnapshotRole"
		policy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		arole, err := ip.CreateAssumeRole(policy, rolename, iamTags)
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- CreateAssumeRole Error", err)
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
		policyarn, err := ip.CreateUserPolicy(policy, policyname, username, iamTags)
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- CreateUserPolicy Error", err)
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
			logger.Error("createUpdateBucketRolesAndPolcies -- policyDoc.ToString Error", err)
			return err
		}
		policyarn, err := ip.CreatePolicyAttachRole(policyname, policy, *snapshotRole, iamTags)
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- CreatePolicyAttachRole Error", err)
			return err
		}
		i.SnapshotPolicyARN = policyarn

	} else {
		// snaphost policy has already been created so we need to add the new statements for this new bucket
		// to the existing policy version.
		_, err := ip.UpdateExistingPolicy(i.SnapshotPolicyARN, []awsiam.PolicyStatementEntry{listStatement, objectStatement})
		if err != nil {
			logger.Error("createUpdateBucketRolesAndPolcies -- UpdateExistingPolicy Error", err)
			return err
		}

	}
	return nil
}

func bindElasticsearchToApp(ctx context.Context, opensearchClient OpensearchClientInterface, ip *awsiam.IAMPolicyClient, settings *config.Settings, logger *slog.Logger, i *ElasticsearchInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		params := &opensearch.DescribeDomainInput{
			DomainName: aws.String(i.Domain), // Required
		}

		resp, err := opensearchClient.DescribeDomain(ctx, params)
		if err != nil {
			logger.Error("bindElasticsearchToApp: UpdateDomainConfig err", err)
			return nil, err
		}

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) {
			if resp.DomainStatus.Endpoints != nil && resp.DomainStatus.ARN != nil {
				logger.Debug(fmt.Sprintf("endpoint: %s ARN: %s \n", resp.DomainStatus.Endpoints["vpc"], *(resp.DomainStatus.ARN)))
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

		err := createUpdateBucketRolesAndPolicies(ip, logger, i, settings.SnapshotsBucketName, i.SnapshotPath, iamTags)
		if err != nil {
			logger.Error("bindElasticsearchToApp - Error in createUpdateRolesAndPolicies", err)
			return nil, err
		}
		i.BrokerSnapshotsEnabled = true
	}

	// add client bucket and adjust policies and roles if present
	if i.Bucket != "" {
		err := createUpdateBucketRolesAndPolicies(ip, logger, i, i.Bucket, "", iamTags)
		if err != nil {
			return nil, err
		}
	}
	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials()
}
