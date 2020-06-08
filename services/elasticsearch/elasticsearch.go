package elasticsearch

import (
	"errors"
	"os"

	"code.cloudfoundry.org/lager"
	"github.com/18F/aws-broker/base"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/cloudfoundry-community/s3-broker/awsiam"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"

	"fmt"
	"log"
)

type ElasticsearchAdapter interface {
	createElasticsearch(i *ElasticsearchInstance, password string) (base.InstanceState, error)
	bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error)
	deleteElasticsearch(i *ElasticsearchInstance) (base.InstanceState, error)
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
	var elasticsearchTags []*elasticsearchservice.Tag

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
		VolumeSize: aws.Int64(10),
		VolumeType: aws.String("standard"),
	}

	esclusterconfig := &elasticsearchservice.ElasticsearchClusterConfig{
		InstanceType:  aws.String("t2.small.elasticsearch"),
		InstanceCount: aws.Int64(2),
	}

	//Standard Parameters
	params := &elasticsearchservice.CreateElasticsearchDomainInput{
		DomainName:                 aws.String(i.Domain),
		ElasticsearchVersion:       aws.String("7.4"),
		EBSOptions:                 ebsoptions,
		ElasticsearchClusterConfig: esclusterconfig,
	}

	resp, err := svc.CreateElasticsearchDomain(params)
	// Pretty-print the response data.
	log.Println(awsutil.StringValue(resp))
	// Decide if AWS service call was successful
	if yes := d.didAwsCallSucceed(err); yes {
		return base.InstanceInProgress, nil
	}
	return base.InstanceNotCreated, nil
}

func (d *dedicatedElasticsearchAdapter) bindElasticsearchToApp(i *ElasticsearchInstance, password string) (map[string]string, error) {
	// First, we need to check if the instance is up and available before binding.
	// Only search for details if the instance was not indicated as ready.
	if i.State != base.InstanceReady {
		svc := elasticsearchservice.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))
		iamsvc := iam.New(session.New(), aws.NewConfig().WithRegion(d.settings.Region))

		logger := lager.NewLogger("aws-broker")
		logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))
		user := awsiam.NewIAMUser(iamsvc, logger)
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
			if resp.DomainStatus.Endpoint != nil && resp.DomainStatus.ARN != nil {
				fmt.Printf("endpoint: %s ARN: %s \n", *(resp.DomainStatus.Endpoint), *(resp.DomainStatus.ARN))
				i.Host = *(resp.DomainStatus.Endpoint)
				i.ARN = *(resp.DomainStatus.ARN)
				i.State = base.InstanceReady
				// Should only be one regardless. Just return now.
			} else {
				// Something went horribly wrong. Should never get here.
				return nil, errors.New("Invalid memory for endpoint and/or endpoint members.")
			}
		} else {
			// Instance not up yet.
			return nil, errors.New("Instance not available yet. Please wait and try again..")
		}

		//IAM User and policy
		if _, err = user.Create(i.Domain, ""); err != nil {
			fmt.Println(err.Error())
			return nil, err
		}

		accessKeyID, secretAccessKey, err := user.CreateAccessKey(i.Domain)
		if err != nil {
			return nil, err
		}

		i.AccessKey = accessKeyID
		i.SecretKey = secretAccessKey

		esARNs := make([]string, 1)
		esARNs[0] = "arn:aws-us-gov:es:us-gov-west-1:135676904304:domain/cg-aws-broker-dev-b780kk"
		policy := `{"Version": "2012-10-17","Statement": [{"Action": ["es:*"],"Effect": "Allow","Resource": {{resources "/*"}}}]}`
		policyARN, err := user.CreatePolicy(i.Domain, "/", policy, esARNs)
		if err != nil {
			return nil, err
		}

		if err = user.AttachUserPolicy(i.Domain, policyARN); err != nil {
			return nil, err
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

	if err := user.Delete(i.Domain); err != nil {
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
