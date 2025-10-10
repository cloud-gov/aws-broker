package rds

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
)

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	// Automigrate!
	db.AutoMigrate(&RDSInstance{}, &base.Instance{}, &jobs.AsyncJobMsg{})
	return db, err
}

type mockParameterGroupClient struct {
	rds              RDSClientInterface
	customPgroupName string
	returnErr        error
}

func (m *mockParameterGroupClient) ProvisionCustomParameterGroupIfNecessary(i *RDSInstance, rdsTags []rdsTypes.Tag) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	i.ParameterGroupName = m.customPgroupName
	return nil
}

func (m *mockParameterGroupClient) CleanupCustomParameterGroups() error {
	return nil
}

type MockDbUtils struct {
	mockFormattedDbName   string
	mockDbName            string
	mockUsername          string
	mockSalt              string
	mockEncryptedPassword string
	mockClearPassword     string
	mockCreds             map[string]string
}

func (m *MockDbUtils) formatDBName(string, string) string {
	return m.mockFormattedDbName
}

func (m *MockDbUtils) getCredentials(i *RDSInstance, password string) (map[string]string, error) {
	return m.mockCreds, nil
}

func (m *MockDbUtils) generateCredentials(settings *config.Settings) (string, string, string, error) {
	return m.mockSalt, m.mockEncryptedPassword, m.mockClearPassword, nil
}

func (m *MockDbUtils) generatePassword(salt string, password string, key string) (string, string, error) {
	return m.mockEncryptedPassword, m.mockClearPassword, nil
}

func (m *MockDbUtils) getPassword(salt string, password string, key string) (string, error) {
	return m.mockClearPassword, nil
}

func (m *MockDbUtils) generateDatabaseName(settings *config.Settings) string {
	return m.mockDbName
}

func (m *MockDbUtils) buildUsername() string {
	return m.mockUsername
}

type mockRDSClient struct {
	createDbErr                         error
	createDBInstanceReadReplicaErr      error
	dbEngineVersions                    []rdsTypes.DBEngineVersion
	describeEngVersionsErr              error
	describeDbParamsErr                 error
	createDbParamGroupErr               error
	deleteDBInstancesCallNum            int
	deleteDbInstancesErrs               []error
	describeEngineDefaultParamsResults  []*rds.DescribeEngineDefaultParametersOutput
	describeEngineDefaultParamsErr      error
	describeEngineDefaultParamsNumPages int
	describeEngineDefaultParamsPageNum  int
	describeDbParamsResults             []*rds.DescribeDBParametersOutput
	describeDbParamsNumPages            int
	describeDbParamsPageNum             int
	describeDBInstancesCallNum          int
	describeDbInstancesResults          []*rds.DescribeDBInstancesOutput
	describeDbInstancesErrs             []error
	modifyDbErrs                        []error
	modifyDbCallNum                     int
	modifyDbParamGroupErr               error
	addTagsToResourceErr                error
}

func (m *mockRDSClient) AddTagsToResource(ctx context.Context, params *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error) {
	if m.addTagsToResourceErr != nil {
		return nil, m.addTagsToResourceErr
	}
	return nil, nil
}

func (m *mockRDSClient) CreateDBInstance(ctx context.Context, params *rds.CreateDBInstanceInput, optFns ...func(*rds.Options)) (*rds.CreateDBInstanceOutput, error) {
	if m.createDbErr != nil {
		return nil, m.createDbErr
	}
	return nil, nil
}

func (m *mockRDSClient) CreateDBInstanceReadReplica(ctx context.Context, params *rds.CreateDBInstanceReadReplicaInput, optFns ...func(*rds.Options)) (*rds.CreateDBInstanceReadReplicaOutput, error) {
	return &rds.CreateDBInstanceReadReplicaOutput{
		DBInstance: &rdsTypes.DBInstance{
			DBInstanceArn: aws.String("arn"),
		},
	}, m.createDBInstanceReadReplicaErr
}

func (m *mockRDSClient) CreateDBParameterGroup(ctx context.Context, params *rds.CreateDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.CreateDBParameterGroupOutput, error) {
	if m.createDbParamGroupErr != nil {
		return nil, m.createDbParamGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) DeleteDBInstance(ctx context.Context, params *rds.DeleteDBInstanceInput, optFns ...func(*rds.Options)) (*rds.DeleteDBInstanceOutput, error) {
	if len(m.deleteDbInstancesErrs) > 0 && m.deleteDbInstancesErrs[m.deleteDBInstancesCallNum] != nil {
		return nil, m.deleteDbInstancesErrs[m.deleteDBInstancesCallNum]
	}
	m.deleteDBInstancesCallNum++
	return nil, nil
}

func (m *mockRDSClient) DeleteDBParameterGroup(ctx context.Context, params *rds.DeleteDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.DeleteDBParameterGroupOutput, error) {
	return nil, nil
}

func (m *mockRDSClient) DescribeDBEngineVersions(ctx context.Context, params *rds.DescribeDBEngineVersionsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBEngineVersionsOutput, error) {
	if m.describeEngVersionsErr != nil {
		return nil, m.describeEngVersionsErr
	}
	if m.dbEngineVersions != nil {
		return &rds.DescribeDBEngineVersionsOutput{
			DBEngineVersions: m.dbEngineVersions,
		}, nil
	}
	return nil, nil
}

func (m *mockRDSClient) DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	if len(m.describeDbInstancesErrs) > 0 && m.describeDbInstancesErrs[m.describeDBInstancesCallNum] != nil {
		return nil, m.describeDbInstancesErrs[m.describeDBInstancesCallNum]
	}
	output := m.describeDbInstancesResults[m.describeDBInstancesCallNum]
	m.describeDBInstancesCallNum++
	return output, nil
}

func (m *mockRDSClient) DescribeDBParameterGroups(ctx context.Context, params *rds.DescribeDBParameterGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParameterGroupsOutput, error) {
	return nil, nil
}

func (m *mockRDSClient) DescribeEngineDefaultParameters(ctx context.Context, params *rds.DescribeEngineDefaultParametersInput, optFns ...func(*rds.Options)) (*rds.DescribeEngineDefaultParametersOutput, error) {
	if m.describeEngineDefaultParamsErr != nil {
		return nil, m.describeEngineDefaultParamsErr
	}
	result := m.describeEngineDefaultParamsResults[m.describeEngineDefaultParamsPageNum]
	m.describeEngineDefaultParamsPageNum++
	return result, nil
}

func (m *mockRDSClient) DescribeDBParameters(ctx context.Context, params *rds.DescribeDBParametersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParametersOutput, error) {
	if m.describeDbParamsErr != nil {
		return nil, m.describeDbParamsErr
	}
	if m.describeDbParamsResults != nil {
		result := m.describeDbParamsResults[m.describeDbParamsPageNum]
		m.describeDbParamsPageNum++
		return result, nil
	}
	return nil, nil
}

func (m *mockRDSClient) ModifyDBParameterGroup(ctx context.Context, params *rds.ModifyDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.ModifyDBParameterGroupOutput, error) {
	if m.modifyDbParamGroupErr != nil {
		return nil, m.modifyDbParamGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) ModifyDBInstance(ctx context.Context, params *rds.ModifyDBInstanceInput, optFns ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error) {
	if len(m.modifyDbErrs) > 0 && m.modifyDbErrs[m.modifyDbCallNum] != nil {
		return nil, m.modifyDbErrs[m.modifyDbCallNum]
	}
	m.modifyDbCallNum++
	return &rds.ModifyDBInstanceOutput{
		DBInstance: &rdsTypes.DBInstance{
			DBInstanceArn: aws.String("arn"),
		},
	}, nil
}
