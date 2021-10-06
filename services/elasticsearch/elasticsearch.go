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

	"fmt"
)

type ElasticsearchAdapter interface {
	createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	modifyElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	checkElasticsearchStatus(i *ElasticsearchInstance) (base.InstanceState, error)
	bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error)
	deleteElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error)
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

func (d *mockElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
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

	//IAM User and policy before domain starts creating so it can be used to create access control policy
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
		var tag elasticsearchservice.Tag
		tag = elasticsearchservice.Tag{
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

	//Standard Parameters
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
		if d.didAwsCallSucceed(err) == false {
			return base.InstanceNotCreated, nil
		}
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

func (d *dedicatedElasticsearchAdapter) modifyElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error) {
	return base.InstanceNotModified, nil
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

		if resp.DomainStatus.Created != nil && *(resp.DomainStatus.Created) == true {
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

	if len(i.Bucket) > 0 {
		iamsvc := iam.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

		assumeRolePolicy := `{"Version": "2012-10-17","Statement": [{"Sid": "","Effect": "Allow","Principal": {"Service": "es.amazonaws.com"},"Action": "sts:AssumeRole"}]}`
		roleInput := &iam.CreateRoleInput{
			AssumeRolePolicyDocument: aws.String(assumeRolePolicy),
			RoleName:                 aws.String(i.Domain + "-to-s3-SnapshotRole"),
		}
		resp, err := iamsvc.CreateRole(roleInput)
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
		if resp.Role.Arn != nil {
			i.SnapshotARN = *(resp.Role.Arn)
			fmt.Println(i.getCredentials(password))
		}
		//Policy to for ES to passRole
		s3Policy := `{"Version": "2012-10-17","Statement": [{"Action": ["s3:ListBucket"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `"]},{"Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Effect": "Allow","Resource": ["arn:aws-us-gov:s3:::` + i.Bucket + `/*"]}]}`
		rolePolicyInput := &iam.CreatePolicyInput{
			PolicyName:     aws.String(i.Domain + "-to-S3-RolePolicy"),
			PolicyDocument: aws.String(s3Policy),
		}

		respPolicy, err := iamsvc.CreatePolicy(rolePolicyInput)
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
		if respPolicy.Policy.Arn != nil && resp.Role.RoleName != nil {
			i.SnapshotPolicyARN = *(respPolicy.Policy.Arn)
			roleAttachPolicyInput := &iam.AttachRolePolicyInput{
				PolicyArn: aws.String(*(respPolicy.Policy.Arn)),
				RoleName:  aws.String(*(resp.Role.RoleName)),
			}

			respAttachPolicy, err := iamsvc.AttachRolePolicy(roleAttachPolicyInput)
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
			fmt.Println(awsutil.StringValue(respAttachPolicy))
		}

		esPermissionPolicy := `{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "iam:PassRole","Resource": "` + i.SnapshotARN + `"},{"Effect": "Allow","Action": "es:ESHttpPut","Resource": "` + i.ARN + `/*"}]}`

		rolePolicyInput = &iam.CreatePolicyInput{
			PolicyName:     aws.String(i.Domain + "-to-S3-ESRolePolicy"),
			PolicyDocument: aws.String(esPermissionPolicy),
		}

		respPolicy, err = iamsvc.CreatePolicy(rolePolicyInput)
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
		fmt.Println(awsutil.StringValue(respPolicy))
		if respPolicy.Policy.Arn != nil {
			i.IamPassRolePolicyARN = *(respPolicy.Policy.Arn)
			userAttachPolicyInput := &iam.AttachUserPolicyInput{
				PolicyArn: aws.String(*(respPolicy.Policy.Arn)),
				UserName:  aws.String(i.Domain),
			}
			_, err := iamsvc.AttachUserPolicy(userAttachPolicyInput)
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
		}
	}
	// If we get here that means the instance is up and we have the information for it.
	return i.getCredentials(password)
}

func (d *dedicatedElasticsearchAdapter) deleteElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error) {
	svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
	iamsvc := iam.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

	logger := lager.NewLogger("aws-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))
	user := awsiam.NewIAMUser(iamsvc, logger)

	if err := user.DetachUserPolicy(i.Domain, i.IamPolicyARN); err != nil {
		return base.InstanceNotGone, err
	}

	if err := user.DeleteAccessKey(i.Domain, i.AccessKey); err != nil {
		return base.InstanceNotGone, err
	}

	if len(i.Bucket) > 0 {
		if err := user.DetachUserPolicy(i.Domain, i.IamPassRolePolicyARN); err != nil {
			fmt.Println(err.Error())
			return base.InstanceNotGone, err
		}

		roleDetachPolicyInput := &iam.DetachRolePolicyInput{
			PolicyArn: aws.String(i.SnapshotPolicyARN),
			RoleName:  aws.String(i.Domain + "-to-s3-SnapshotRole"),
		}

		if _, err := iamsvc.DetachRolePolicy(roleDetachPolicyInput); err != nil {
			fmt.Println(err.Error())
			return base.InstanceNotGone, err
		}

		if err := user.DeletePolicy(i.SnapshotPolicyARN); err != nil {
			fmt.Println(err.Error())
			return base.InstanceNotGone, err
		}

		rolePolicyDeleteInput := &iam.DeleteRoleInput{
			RoleName: aws.String(i.Domain + "-to-s3-SnapshotRole"),
		}

		if _, err := iamsvc.DeleteRole(rolePolicyDeleteInput); err != nil {
			fmt.Println(err.Error())
			return base.InstanceNotGone, err
		}

		if err := user.DeletePolicy(i.IamPassRolePolicyARN); err != nil {
			fmt.Println(err.Error())
			return base.InstanceNotGone, err
		}
	}

	if err := user.Delete(i.Domain); err != nil {
		fmt.Println(err.Error())
		return base.InstanceNotGone, err
	}

	if err := user.DeletePolicy(i.IamPolicyARN); err != nil {
		fmt.Println(err.Error())
		return base.InstanceNotGone, err
	}

	params := &elasticsearchservice.DeleteElasticsearchDomainInput{
		DomainName: aws.String(i.Domain), // Required
	}
	resp, err := svc.DeleteElasticsearchDomain(params)
	// Pretty-print the response data.
	fmt.Println(awsutil.StringValue(resp))

	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceGone, nil
	}
	return base.InstanceNotGone, nil
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
