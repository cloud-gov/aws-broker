package rds

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/testutil"
	"gorm.io/gorm"
)

func testDBInit() (*gorm.DB, error) {
	db, err := testutil.TestDbInit()
	// Automigrate!
	err = db.AutoMigrate(&RDSInstance{}, &base.Instance{}, &asyncmessage.AsyncJobMsg{})
	return db, err
}

func createTestRdsInstance(i *RDSInstance) *RDSInstance {
	if i.credentialUtils == nil {
		i.credentialUtils = &RDSCredentialUtils{}
	}
	return i
}

type mockParameterGroupClient struct {
	rds                            RDSClientInterface
	customPgroupName               string
	provisionNewParamGroupErr      error
	provisionOrModifyParamGroupErr error
	deleteParameterGroupErr        error
	isCustomParameterGroup         bool
	reconciledInstance             *RDSInstance
}

func (m *mockParameterGroupClient) ProvisionNewCustomParameterGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) error {
	if m.provisionNewParamGroupErr != nil {
		return m.provisionNewParamGroupErr
	}
	i.ParameterGroupName = m.customPgroupName
	return nil
}

func (m *mockParameterGroupClient) ProvisionOrModifyCustomParameterGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) (bool, error) {
	if m.provisionOrModifyParamGroupErr != nil {
		return false, m.provisionOrModifyParamGroupErr
	}
	i.ParameterGroupName = m.customPgroupName
	return false, nil
}

func (m *mockParameterGroupClient) CleanupCustomParameterGroups() error {
	return nil
}

func (m *mockParameterGroupClient) DeleteParameterGroup(oldParameterGroupName string) error {
	return m.deleteParameterGroupErr
}

func (m *mockParameterGroupClient) IsCustomParameterGroup(parameterGroupName string) bool {
	return m.isCustomParameterGroup
}

func (m *mockParameterGroupClient) ReconcileRDSInstanceParameterGroup(dbInstanceState *rdsTypes.DBInstance, i RDSInstance) (*RDSInstance, error) {
	if m.reconciledInstance != nil {
		return m.reconciledInstance, nil
	}
	return &i, nil
}

type mockOptionGroupClient struct {
	optionGroupName              string
	provisionOrModifyCalled      bool
	provisionOrModifyCreated     bool
	provisionOrModifyOptGroupErr error
	deleteOptionGroupErr         error
	deletedOptionGroupName       string
	isCustomOptionGroup          bool
	isBrokerOptionGroup          bool
	reconciledInstance           *RDSInstance
}

func (m *mockOptionGroupClient) ProvisionOrModifyCustomOptionGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) (bool, error) {
	m.provisionOrModifyCalled = true
	if m.provisionOrModifyOptGroupErr != nil {
		return false, m.provisionOrModifyOptGroupErr
	}
	if m.optionGroupName != "" {
		i.OptionGroupName = m.optionGroupName
	}
	return m.provisionOrModifyCreated, nil
}

func (m *mockOptionGroupClient) CleanupCustomOptionGroups() error {
	return nil
}

func (m *mockOptionGroupClient) DeleteOptionGroup(optionGroupName string) error {
	m.deletedOptionGroupName = optionGroupName
	return m.deleteOptionGroupErr
}

func (m *mockOptionGroupClient) IsCustomOptionGroup(optionGroupName string) bool {
	return m.isCustomOptionGroup
}

func (m *mockOptionGroupClient) IsBrokerOptionGroup(optionGroupName string) bool {
	return m.isBrokerOptionGroup
}

func (m *mockOptionGroupClient) ReconcileRDSInstanceOptionGroup(dbInstanceState *rdsTypes.DBInstance, i RDSInstance) (*RDSInstance, error) {
	if m.reconciledInstance != nil {
		return m.reconciledInstance, nil
	}
	return &i, nil
}

type mockCredentialUtils struct {
	mockSalt              string
	mockEncryptedPassword string
	mockClearPassword     string
	mockGetPassworrdErr   error
	mockCreds             map[string]string
}

func (m *mockCredentialUtils) getCredentials(i *RDSInstance, password string) (map[string]string, error) {
	return m.mockCreds, nil
}

func (m *mockCredentialUtils) generateCredentials(settings *config.Settings) (string, string, error) {
	return m.mockSalt, m.mockEncryptedPassword, nil
}

func (m *mockCredentialUtils) generatePassword(salt string, password string, key string) (string, error) {
	return m.mockEncryptedPassword, nil
}

func (m *mockCredentialUtils) getPassword(salt string, password string, key string) (string, error) {
	return m.mockClearPassword, m.mockGetPassworrdErr
}

type mockRDSClient struct {
	createDbErr                         error
	createDBInstanceReadReplicaErrs     []error
	createDBInstanceReadReplicaCallNum  int
	dbEngineVersions                    []rdsTypes.DBEngineVersion
	describeEngVersionsErr              error
	describeDbParamsErrs                []error
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
	describeDBParameterGroupsOutput     []*rds.DescribeDBParameterGroupsOutput
	describeDBParameterGroupsCallNum    int
	deleteDbParameterGroupErrs          []error
	deleteDbParameterGroupCallNum       int
	describeOptionGroupsResults         []*rds.DescribeOptionGroupsOutput
	describeOptionGroupsErrs            []error
	describeOptionGroupsCallNum         int
	createOptionGroupInput              *rds.CreateOptionGroupInput
	createOptionGroupErr                error
	modifyOptionGroupInput              *rds.ModifyOptionGroupInput
	modifyOptionGroupErr                error
	deleteOptionGroupErrs               []error
	deleteOptionGroupCallNum            int
}

func (m *mockRDSClient) CreateOptionGroup(ctx context.Context, params *rds.CreateOptionGroupInput, optFns ...func(*rds.Options)) (*rds.CreateOptionGroupOutput, error) {
	m.createOptionGroupInput = params
	if m.createOptionGroupErr != nil {
		return nil, m.createOptionGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) ModifyOptionGroup(ctx context.Context, params *rds.ModifyOptionGroupInput, optFns ...func(*rds.Options)) (*rds.ModifyOptionGroupOutput, error) {
	m.modifyOptionGroupInput = params
	if m.modifyOptionGroupErr != nil {
		return nil, m.modifyOptionGroupErr
	}
	return nil, nil
}

func (m *mockRDSClient) DeleteOptionGroup(ctx context.Context, params *rds.DeleteOptionGroupInput, optFns ...func(*rds.Options)) (*rds.DeleteOptionGroupOutput, error) {
	var err error
	if len(m.deleteOptionGroupErrs) > m.deleteOptionGroupCallNum && m.deleteOptionGroupErrs[m.deleteOptionGroupCallNum] != nil {
		err = m.deleteOptionGroupErrs[m.deleteOptionGroupCallNum]
	}
	m.deleteOptionGroupCallNum++
	return nil, err
}

func (m *mockRDSClient) DescribeOptionGroups(ctx context.Context, params *rds.DescribeOptionGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeOptionGroupsOutput, error) {
	var err error
	if len(m.describeOptionGroupsErrs) > m.describeOptionGroupsCallNum && m.describeOptionGroupsErrs[m.describeOptionGroupsCallNum] != nil {
		err = m.describeOptionGroupsErrs[m.describeOptionGroupsCallNum]
	}
	var result *rds.DescribeOptionGroupsOutput
	if len(m.describeOptionGroupsResults) > m.describeOptionGroupsCallNum {
		result = m.describeOptionGroupsResults[m.describeOptionGroupsCallNum]
	}
	m.describeOptionGroupsCallNum++
	return result, err
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
	var err error
	if m.createDBInstanceReadReplicaCallNum >= 0 && m.createDBInstanceReadReplicaCallNum < len(m.createDBInstanceReadReplicaErrs) {
		err = m.createDBInstanceReadReplicaErrs[m.createDBInstanceReadReplicaCallNum]
	}
	m.createDBInstanceReadReplicaCallNum++
	return &rds.CreateDBInstanceReadReplicaOutput{
		DBInstance: &rdsTypes.DBInstance{
			DBInstanceArn: aws.String("arn"),
		},
	}, err
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
	var err error
	if len(m.deleteDbParameterGroupErrs) > 0 && m.deleteDbParameterGroupErrs[m.deleteDbParameterGroupCallNum] != nil {
		err = m.deleteDbParameterGroupErrs[m.deleteDbParameterGroupCallNum]
	}
	m.deleteDbParameterGroupCallNum++
	return nil, err
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
	output := m.describeDBParameterGroupsOutput[m.describeDBParameterGroupsCallNum]
	m.describeDBParameterGroupsCallNum++
	return output, nil
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
	var err error
	var result *rds.DescribeDBParametersOutput
	if len(m.describeDbParamsErrs) > m.describeDbParamsPageNum && m.describeDbParamsErrs[m.describeDbParamsPageNum] != nil {
		err = m.describeDbParamsErrs[m.describeDbParamsPageNum]
	}
	if len(m.describeDbParamsResults) > m.describeDbParamsPageNum && m.describeDbParamsResults[m.describeDbParamsPageNum] != nil {
		result = m.describeDbParamsResults[m.describeDbParamsPageNum]
	}
	m.describeDbParamsPageNum++
	return result, err
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

type mockLogHandler struct {
	Records []slog.Record
}

func (h *mockLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *mockLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.Records = append(h.Records, r)
	return nil
}
func (h *mockLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *mockLogHandler) WithGroup(name string) slog.Handler       { return h }
