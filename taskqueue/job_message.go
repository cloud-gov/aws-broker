package taskqueue

import (
	"fmt"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/jinzhu/gorm"
)

func CreateAsyncJobMessage(db *gorm.DB, brokerId string, instanceId string, operation base.Operation, state base.InstanceState, message string) error {
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
	// TODO: better handling of this error
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func UpdateAsyncJobMessage(db *gorm.DB, brokerId string, instanceId string, operation base.Operation, state base.InstanceState, message string) error {
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
	// TODO: better handling of this error
	if err != nil {
		fmt.Println(err)
	}
	return err
}
