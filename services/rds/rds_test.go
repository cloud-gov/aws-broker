package rds

import (
	"errors"
	"testing"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/go-test/deep"
)

type mockParameterGroupClient struct {
	rds              rdsiface.RDSAPI
	customPgroupName string
	returnErr        error
}

func (m *mockParameterGroupClient) ProvisionCustomParameterGroupIfNecessary(i *RDSInstance) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	i.ParameterGroupName = m.customPgroupName
	return nil
}

func (m *mockParameterGroupClient) CleanupCustomParameterGroups() {}

type mockRdsClientForAdapterTests struct {
	rdsiface.RDSAPI

	createDbErr error
	modifyDbErr error
}

func (m mockRdsClientForAdapterTests) CreateDBInstance(*rds.CreateDBInstanceInput) (*rds.CreateDBInstanceOutput, error) {
	if m.createDbErr != nil {
		return nil, m.createDbErr
	}
	return nil, nil
}

func (m mockRdsClientForAdapterTests) ModifyDBInstance(*rds.ModifyDBInstanceInput) (*rds.ModifyDBInstanceOutput, error) {
	if m.modifyDbErr != nil {
		return nil, m.modifyDbErr
	}
	return nil, nil
}

func TestPrepareCreateDbInstanceInput(t *testing.T) {
	testErr := errors.New("fail")
	testCases := map[string]struct {
		dbInstance        *RDSInstance
		dbAdapter         *dedicatedDBAdapter
		expectedGroupName string
		expectedErr       error
		password          string
	}{
		"expect returned group name": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					customPgroupName: "foobar",
					rds:              &mockRDSClient{},
				},
			},
			expectedGroupName: "foobar",
		},
		"expect error": {
			dbInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
				DbType:          "mysql",
				dbUtils:         &RDSDatabaseUtils{},
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					returnErr: testErr,
					rds:       &mockRDSClient{},
				},
			},
			expectedErr: testErr,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := test.dbAdapter.prepareCreateDbInput(test.dbInstance, test.password)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedErr == nil && *params.DBParameterGroupName != test.expectedGroupName {
				t.Fatalf("expected group name: %s, got: %s", test.expectedGroupName, *params.DBParameterGroupName)
			}
		})
	}
}

func TestCreateDb(t *testing.T) {
	createDbErr := errors.New("create DB error")
	testCases := map[string]struct {
		dbInstance           *RDSInstance
		dbAdapter            dbAdapter
		expectedErr          error
		expectedResponseCode base.InstanceState
		password             string
	}{
		"create DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					createDbErr: createDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
			},
			dbInstance:           NewRDSInstance(),
			expectedErr:          createDbErr,
			expectedResponseCode: base.InstanceNotCreated,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.dbAdapter.createDB(test.dbInstance, test.password)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if responseCode != test.expectedResponseCode {
				t.Errorf("expected response: %s, got: %s", test.expectedResponseCode, responseCode)
			}
		})
	}
}

func TestModifyDb(t *testing.T) {
	modifyDbErr := errors.New("modify DB error")
	testCases := map[string]struct {
		dbInstance           *RDSInstance
		dbAdapter            dbAdapter
		expectedErr          error
		expectedResponseCode base.InstanceState
		password             string
	}{
		"modify DB error": {
			dbAdapter: &dedicatedDBAdapter{
				rds: &mockRdsClientForAdapterTests{
					modifyDbErr: modifyDbErr,
				},
				parameterGroupClient: &mockParameterGroupClient{},
			},
			dbInstance:           NewRDSInstance(),
			expectedErr:          modifyDbErr,
			expectedResponseCode: base.InstanceNotModified,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			responseCode, err := test.dbAdapter.modifyDB(test.dbInstance, test.password)
			if err != nil && test.expectedErr == nil {
				t.Errorf("unexpected error: %s", err)
			}
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("expected error: %s, got: %s", test.expectedErr, err)
			}
			if responseCode != test.expectedResponseCode {
				t.Errorf("expected response: %s, got: %s", test.expectedResponseCode, responseCode)
			}
		})
	}
}

func TestPrepareModifyDbInstanceInput(t *testing.T) {
	testErr := errors.New("fail")
	testCases := map[string]struct {
		dbInstance        *RDSInstance
		dbAdapter         *dedicatedDBAdapter
		expectedGroupName string
		expectedErr       error
		expectedParams    *rds.ModifyDBInstanceInput
	}{
		"expect returned group name": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				dbUtils:               &RDSDatabaseUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					customPgroupName: "foobar",
					rds:              &mockRDSClient{},
				},
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
			},
			expectedGroupName: "foobar",
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int64(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int64(14),
				DBParameterGroupName:     aws.String("foobar"),
			},
		},
		"expect error": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				dbUtils:               &RDSDatabaseUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					rds:       &mockRDSClient{},
					returnErr: testErr,
				},
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
			},
			expectedErr: testErr,
		},
		"update password": {
			dbInstance: &RDSInstance{
				BinaryLogFormat:       "ROW",
				DbType:                "mysql",
				ClearPassword:         "fake-pw",
				dbUtils:               &RDSDatabaseUtils{},
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				rds: &mockRDSClient{},
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int64(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int64(14),
				MasterUserPassword:       aws.String("fake-pw"),
			},
		},
		"update storage type": {
			dbInstance: &RDSInstance{
				dbUtils:               &RDSDatabaseUtils{},
				DbType:                "mysql",
				StorageType:           "gp3",
				AllocatedStorage:      20,
				Database:              "db-name",
				BackupRetentionPeriod: 14,
			},
			dbAdapter: &dedicatedDBAdapter{
				Plan: catalog.RDSPlan{
					InstanceClass: "class",
					Redundant:     true,
				},
				parameterGroupClient: &mockParameterGroupClient{
					rds: &mockRDSClient{},
				},
				rds: &mockRDSClient{},
			},
			expectedParams: &rds.ModifyDBInstanceInput{
				AllocatedStorage:         aws.Int64(20),
				ApplyImmediately:         aws.Bool(true),
				DBInstanceClass:          aws.String("class"),
				MultiAZ:                  aws.Bool(true),
				DBInstanceIdentifier:     aws.String("db-name"),
				AllowMajorVersionUpgrade: aws.Bool(false),
				BackupRetentionPeriod:    aws.Int64(14),
				StorageType:              aws.String("gp3"),
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			params, err := test.dbAdapter.prepareModifyDbInstanceInput(test.dbInstance)
			if !errors.Is(test.expectedErr, err) {
				t.Errorf("unexpected error: %s", err)
			}
			if test.expectedParams != nil {
				if diff := deep.Equal(params, test.expectedParams); diff != nil {
					t.Error(diff)
				}
			}
			if test.expectedParams != nil {
				if diff := deep.Equal(params, test.expectedParams); diff != nil {
					t.Error(diff)
				}
			}
		})
	}
}
