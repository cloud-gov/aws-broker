package elasticsearch

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/opensearchservice"
	"github.com/go-test/deep"
)

func TestIsInvalidTypeException(t *testing.T) {
	isInvalidType := isInvalidTypeException(&opensearchservice.InvalidTypeException{})
	if !isInvalidType {
		t.Fatal("expected isInvalidTypeException() to return true")
	}
}

func TestPrepareCreateDomainInput(t *testing.T) {
	testCases := map[string]struct {
		esInstance     *ElasticsearchInstance
		accessPolicy   string
		expectedParams *opensearchservice.CreateDomainInput
	}{
		"data count of 1": {
			esInstance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				DataCount:                  1,
				SubnetID2AZ2:               "az-2",
				SecGroup:                   "group-1",
				EncryptAtRest:              false,
				VolumeSize:                 10,
				VolumeType:                 "gp3",
				InstanceType:               "db.m5.xlarge",
				NodeToNodeEncryption:       true,
				AutomatedSnapshotStartHour: 0,
			},
			accessPolicy: "fake-access-policy",
			expectedParams: &opensearchservice.CreateDomainInput{
				DomainName:     aws.String("test-domain"),
				AccessPolicies: aws.String("fake-access-policy"),
				VPCOptions: &opensearchservice.VPCOptions{
					SubnetIds:        []*string{aws.String("az-2")},
					SecurityGroupIds: []*string{aws.String("group-1")},
				},
				DomainEndpointOptions: &opensearchservice.DomainEndpointOptions{
					EnforceHTTPS: aws.Bool(true),
				},
				EBSOptions: &opensearchservice.EBSOptions{
					EBSEnabled: aws.Bool(true),
					VolumeSize: aws.Int64(int64(10)),
					VolumeType: aws.String("gp3"),
				},
				ClusterConfig: &opensearchservice.ClusterConfig{
					InstanceType:  aws.String("db.m5.xlarge"),
					InstanceCount: aws.Int64(int64(1)),
				},
				SnapshotOptions: &opensearchservice.SnapshotOptions{
					AutomatedSnapshotStartHour: aws.Int64(int64(0)),
				},
				NodeToNodeEncryptionOptions: &opensearchservice.NodeToNodeEncryptionOptions{
					Enabled: aws.Bool(true),
				},
				EncryptionAtRestOptions: &opensearchservice.EncryptionAtRestOptions{
					Enabled: aws.Bool(false),
				},
			},
		},
		"data count is greater than 1": {
			esInstance: &ElasticsearchInstance{
				Domain:                     "test-domain",
				DataCount:                  2,
				SubnetID3AZ1:               "az-3",
				SubnetID4AZ2:               "az-4",
				SecGroup:                   "group-1",
				EncryptAtRest:              false,
				VolumeSize:                 10,
				VolumeType:                 "gp3",
				InstanceType:               "db.m5.xlarge",
				NodeToNodeEncryption:       true,
				AutomatedSnapshotStartHour: 0,
			},
			accessPolicy: "fake-access-policy",
			expectedParams: &opensearchservice.CreateDomainInput{
				DomainName:     aws.String("test-domain"),
				AccessPolicies: aws.String("fake-access-policy"),
				VPCOptions: &opensearchservice.VPCOptions{
					SubnetIds:        []*string{aws.String("az-3"), aws.String("az-4")},
					SecurityGroupIds: []*string{aws.String("group-1")},
				},
				DomainEndpointOptions: &opensearchservice.DomainEndpointOptions{
					EnforceHTTPS: aws.Bool(true),
				},
				EBSOptions: &opensearchservice.EBSOptions{
					EBSEnabled: aws.Bool(true),
					VolumeSize: aws.Int64(int64(10)),
					VolumeType: aws.String("gp3"),
				},
				ClusterConfig: &opensearchservice.ClusterConfig{
					InstanceType:         aws.String("db.m5.xlarge"),
					InstanceCount:        aws.Int64(int64(2)),
					ZoneAwarenessEnabled: aws.Bool(true),
					ZoneAwarenessConfig: &opensearchservice.ZoneAwarenessConfig{
						AvailabilityZoneCount: aws.Int64(int64(2)),
					},
				},
				SnapshotOptions: &opensearchservice.SnapshotOptions{
					AutomatedSnapshotStartHour: aws.Int64(int64(0)),
				},
				NodeToNodeEncryptionOptions: &opensearchservice.NodeToNodeEncryptionOptions{
					Enabled: aws.Bool(true),
				},
				EncryptionAtRestOptions: &opensearchservice.EncryptionAtRestOptions{
					Enabled: aws.Bool(false),
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params := prepareCreateDomainInput(
				test.esInstance,
				test.accessPolicy,
			)
			if diff := deep.Equal(params, test.expectedParams); diff != nil {
				t.Error(diff)
			}
		})
	}
}

func TestPrepareUpdateDomainConfigInput(t *testing.T) {
	testCases := map[string]struct {
		esInstance     *ElasticsearchInstance
		expectedParams *opensearchservice.UpdateDomainConfigInput
	}{
		"no ebs options": {
			esInstance: &ElasticsearchInstance{
				Domain: "fake-domain",
			},
			expectedParams: &opensearchservice.UpdateDomainConfigInput{
				DomainName:      aws.String("fake-domain"),
				AdvancedOptions: map[string]*string{},
			},
		},
		"update volume type": {
			esInstance: &ElasticsearchInstance{
				Domain:     "fake-domain",
				VolumeType: "gp3",
				VolumeSize: 15,
			},
			expectedParams: &opensearchservice.UpdateDomainConfigInput{
				DomainName:      aws.String("fake-domain"),
				AdvancedOptions: map[string]*string{},
				EBSOptions: &opensearchservice.EBSOptions{
					EBSEnabled: aws.Bool(true),
					VolumeType: aws.String("gp3"),
					VolumeSize: aws.Int64(15),
				},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params := prepareUpdateDomainConfigInput(test.esInstance)
			if diff := deep.Equal(params, test.expectedParams); diff != nil {
				t.Error(diff)
			}
		})
	}
}
