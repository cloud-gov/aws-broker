package rds

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
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
	rds              rdsiface.RDSAPI
	customPgroupName string
	returnErr        error
}

func (m *mockParameterGroupClient) ProvisionCustomParameterGroupIfNecessary(i *RDSInstance, rdsTags []*rds.Tag) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	i.ParameterGroupName = m.customPgroupName
	return nil
}

func (m *mockParameterGroupClient) CleanupCustomParameterGroups() {}

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
	rdsiface.RDSAPI

	createDbErr                         error
	createDBInstanceReadReplicaErr      error
	dbEngineVersions                    []*rds.DBEngineVersion
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

func (m *mockRDSClient) CreateDBInstance(*rds.CreateDBInstanceInput) (*rds.CreateDBInstanceOutput, error) {
	if m.createDbErr != nil {
		return nil, m.createDbErr
	}
	return nil, nil
}

func (m *mockRDSClient) DescribeDBParameters(*rds.DescribeDBParametersInput) (*rds.DescribeDBParametersOutput, error) {
	if m.describeDbParamsErr != nil {
		return nil, m.describeDbParamsErr
	}
	return nil, nil
}

func (m *mockRDSClient) DescribeDBEngineVersions(*rds.DescribeDBEngineVersionsInput) (*rds.DescribeDBEngineVersionsOutput, error) {
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

func (m *mockRDSClient) CreateDBParameterGroup(*rds.CreateDBParameterGroupInput) (*rds.CreateDBParameterGroupOutput, error) {
	if m.createDbParamGroupErr != nil {
		return nil, m.createDbParamGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) ModifyDBParameterGroup(*rds.ModifyDBParameterGroupInput) (*rds.DBParameterGroupNameMessage, error) {
	if m.modifyDbParamGroupErr != nil {
		return nil, m.modifyDbParamGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) DescribeEngineDefaultParametersPages(input *rds.DescribeEngineDefaultParametersInput, fn func(*rds.DescribeEngineDefaultParametersOutput, bool) bool) error {
	if m.describeEngineDefaultParamsErr != nil {
		return m.describeEngineDefaultParamsErr
	}
	shouldContinue := true
	for shouldContinue {
		output := m.describeEngineDefaultParamsResults[m.describeEngineDefaultParamsPageNum]
		m.describeEngineDefaultParamsPageNum++
		lastPage := m.describeEngineDefaultParamsPageNum == m.describeEngineDefaultParamsNumPages
		shouldContinue = fn(output, lastPage)
	}
	return nil
}

func (m *mockRDSClient) DescribeDBParametersPages(input *rds.DescribeDBParametersInput, fn func(*rds.DescribeDBParametersOutput, bool) bool) error {
	if m.describeDbParamsErr != nil {
		return m.describeDbParamsErr
	}
	shouldContinue := true
	for shouldContinue {
		output := m.describeDbParamsResults[m.describeDbParamsPageNum]
		m.describeDbParamsPageNum++
		lastPage := m.describeDbParamsPageNum == m.describeDbParamsNumPages
		shouldContinue = fn(output, lastPage)
	}
	return nil
}

func (m *mockRDSClient) DescribeDBInstances(input *rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error) {
	if len(m.describeDbInstancesErrs) > 0 && m.describeDbInstancesErrs[m.describeDBInstancesCallNum] != nil {
		return nil, m.describeDbInstancesErrs[m.describeDBInstancesCallNum]
	}
	output := m.describeDbInstancesResults[m.describeDBInstancesCallNum]
	m.describeDBInstancesCallNum++
	return output, nil
}

func (m *mockRDSClient) CreateDBInstanceReadReplica(*rds.CreateDBInstanceReadReplicaInput) (*rds.CreateDBInstanceReadReplicaOutput, error) {
	return &rds.CreateDBInstanceReadReplicaOutput{
		DBInstance: &rds.DBInstance{
			DBInstanceArn: aws.String("arn"),
		},
	}, m.createDBInstanceReadReplicaErr
}

func (m *mockRDSClient) ModifyDBInstance(*rds.ModifyDBInstanceInput) (*rds.ModifyDBInstanceOutput, error) {
	if len(m.modifyDbErrs) > 0 && m.modifyDbErrs[m.modifyDbCallNum] != nil {
		return nil, m.modifyDbErrs[m.modifyDbCallNum]
	}
	m.modifyDbCallNum++
	return nil, nil
}

func (m *mockRDSClient) DeleteDBInstance(*rds.DeleteDBInstanceInput) (*rds.DeleteDBInstanceOutput, error) {
	if len(m.deleteDbInstancesErrs) > 0 && m.deleteDbInstancesErrs[m.deleteDBInstancesCallNum] != nil {
		return nil, m.deleteDbInstancesErrs[m.deleteDBInstancesCallNum]
	}
	m.deleteDBInstancesCallNum++
	return nil, nil
}

func (m *mockRDSClient) AddTagsToResource(*rds.AddTagsToResourceInput) (*rds.AddTagsToResourceOutput, error) {
	if m.addTagsToResourceErr != nil {
		return nil, m.addTagsToResourceErr
	}
	return nil, nil
}
