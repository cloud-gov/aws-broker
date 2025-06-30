package async_jobs

import (
	"errors"
	"fmt"

	"github.com/cloud-gov/aws-broker/base"
	"gorm.io/gorm"
)

// This function is writing a message to the database for tracking the state of an asychronous job. This is useful
// when querying the status of asynchronous create/modify/delete operations from a LastOperation handler.
func WriteAsyncJobMessage(db *gorm.DB, brokerId string, instanceId string, operation base.Operation, state base.InstanceState, message string) error {
	asyncJobMsg := &AsyncJobMsg{
		BrokerId:   brokerId,
		InstanceId: instanceId,
		JobType:    operation,
		JobState: AsyncJobState{
			Message: message,
			State:   state,
		},
	}
	err := db.Save(asyncJobMsg).Error
	return err
}

func GetLastAsyncJobMessage(db *gorm.DB, brokerId string, instanceId string, operation base.Operation) (*AsyncJobMsg, error) {
	asyncJobMsg := AsyncJobMsg{}
	result := db.Where("broker_id = ?", brokerId).Where("instance_id = ?", instanceId).Where("job_type = ?", operation).First(&asyncJobMsg)
	if result.RowsAffected == 0 {
		return nil, errors.New("could not find async job status message")
	}
	return &asyncJobMsg, result.Error
}

func ShouldWriteAsyncJobMessage(db *gorm.DB, brokerId string, instanceId string, operation base.Operation, state base.InstanceState, message string) {
	err := WriteAsyncJobMessage(db, brokerId, instanceId, operation, state, message)
	if err != nil {
		fmt.Println(fmt.Errorf("ShouldWriteAsyncJobMessage: %w", err))
	}
}
