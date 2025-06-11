package rds

import (
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/taskqueue"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/go-co-op/gocron"
)

type mockTagManager struct{}

func (t *mockTagManager) GenerateTags(
	action brokertags.Action,
	serviceName string,
	servicePlanName string,
	resourceGUIDs brokertags.ResourceGUIDs,
	getMissingResources bool,
) (map[string]string, error) {
	return nil, nil
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

type mockRdsClientForAdapterTests struct {
	rdsiface.RDSAPI

	createDbErr                    error
	modifyDbErr                    error
	describeDBInstancesCallNum     int
	describeDBInstancesResponses   []*string
	describeDBInstancesErr         error
	createDBInstanceReadReplicaErr error
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

func (m *mockRdsClientForAdapterTests) DescribeDBInstances(*rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error) {
	if m.describeDBInstancesErr != nil {
		return nil, m.describeDBInstancesErr
	}
	output := &rds.DescribeDBInstancesOutput{
		DBInstances: []*rds.DBInstance{
			{
				DBInstanceStatus: m.describeDBInstancesResponses[m.describeDBInstancesCallNum],
			},
		},
	}
	m.describeDBInstancesCallNum++
	return output, nil
}

func (m mockRdsClientForAdapterTests) CreateDBInstanceReadReplica(*rds.CreateDBInstanceReadReplicaInput) (*rds.CreateDBInstanceReadReplicaOutput, error) {
	return nil, m.createDBInstanceReadReplicaErr
}

type mockQueueManager struct {
	jobChan   chan taskqueue.AsyncJobMsg
	taskState *taskqueue.AsyncJobState
}

func (q mockQueueManager) ScheduleTask(cronExpression string, id string, task interface{}) (*gocron.Job, error) {
	return nil, nil
}

func (q mockQueueManager) UnScheduleTask(id string) error {
	return nil
}

func (q mockQueueManager) IsTaskScheduled(id string) bool {
	return false
}

func (q mockQueueManager) RequestTaskQueue(brokerid string, instanceid string, operation base.Operation) (chan taskqueue.AsyncJobMsg, error) {
	return q.jobChan, nil
}

func (q mockQueueManager) GetTaskState(brokerid string, instanceid string, operation base.Operation) (*taskqueue.AsyncJobState, error) {
	return q.taskState, nil
}

type MockDbUtils struct {
	mockFormattedDbName   string
	mockDbName            string
	mockUsername          string
	mockSalt              string
	mockEncryptedPassword string
	mockClearPassword     string
}

func (m *MockDbUtils) FormatDBName(string, string) string {
	return m.mockFormattedDbName
}

func (m *MockDbUtils) getCredentials(i *RDSInstance, password string) (map[string]string, error) {
	return nil, nil
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
