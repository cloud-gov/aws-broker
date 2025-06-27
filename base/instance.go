package base

import (
	"log"
	"net/http"
	"time"

	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/helpers/response"
	"gorm.io/gorm"
)

// InstanceState is an enumeration to indicate what state the instance is in.
type InstanceState uint8

const (
	// InstanceNotCreated is the default InstanceState that represents an uninitiated instance.
	InstanceNotCreated InstanceState = iota // 0
	// InstanceInProgress indicates that the instance is in a intermediate step.
	InstanceInProgress // 1
	// InstanceReady indicates that the instance is created or modified and ready to be used.
	InstanceReady // 2
	// InstanceGone indicates that the instance is deleted.
	InstanceGone // 3
	// InstanceNotGone indicates that the instance is not deleted.
	InstanceNotGone // 4
	// InstanceNotModified indicates that the instance is not modified.
	InstanceNotModified // 5
)

func (i InstanceState) String() string {
	switch i {
	case InstanceNotCreated:
		return "not created"
	case InstanceInProgress:
		return "in progress"
	case InstanceReady:
		return "ready"
	case InstanceGone:
		return "deleted"
	case InstanceNotGone:
		return "not deleted"
	case InstanceNotModified:
		return "not modified"
	default:
		return "unknown"
	}
}

// Convert the instance state to a valid LastOperation status
//
// Valid values for a LastOperation response in the Open Service Broker API spec:
//
//	https://github.com/cloudfoundry/servicebroker/blob/master/spec.md#body-1
func (i InstanceState) ToLastOperationStatus() string {
	switch i {
	case InstanceInProgress:
		return "in progress"
	case InstanceReady, InstanceGone:
		return "succeeded"
	case InstanceNotCreated, InstanceNotModified, InstanceNotGone:
		return "failed"
	default:
		return "in progress"
	}
}

type Instance struct {
	Uuid string `gorm:"primaryKey" sql:"type:varchar(255) PRIMARY KEY"`

	request.Request

	Host string `sql:"size(255)"`
	Port int64

	State InstanceState

	CreatedAt time.Time
	UpdatedAt time.Time
}

// FindBaseInstance is a helper function to find the base instance of the
func FindBaseInstance(brokerDb *gorm.DB, id string) (Instance, response.Response) {
	instance := Instance{}
	log.Println("Looking for instance with id " + id)
	result := brokerDb.Where("uuid = ?", id).First(&instance)
	if result.Error == nil {
		return instance, nil
	} else if result.RowsAffected == 0 {
		return instance, response.NewErrorResponse(http.StatusNotFound, result.Error.Error())
	} else {
		return instance, response.NewErrorResponse(http.StatusInternalServerError, result.Error.Error())
	}
}
