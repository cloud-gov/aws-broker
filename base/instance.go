package base

import (
	"github.com/18F/aws-broker/common/request"
	"github.com/18F/aws-broker/common/response"
	"github.com/jinzhu/gorm"
	"log"
	"net/http"
	"time"
)

// InstanceState is an enumeration to indicate what state the instance is in.
type InstanceState uint8

const (
	// InstanceNotCreated is the default InstanceState that represents an uninitiated instance.
	InstanceNotCreated InstanceState = iota // 0
	// InstanceInProgress indicates that the instance is in a intermediate step.
	InstanceInProgress // 1
	// InstanceReady indicates that the instance is created and ready to be used.
	InstanceReady // 2
	// InstanceGone indicates that the instance is deleted.
	InstanceGone // 3
	// InstanceNotGone indicates that the instance is successfully deleted.
	InstanceNotGone // 4
)

// Instance is the base that all instances should include
type Instance struct {
	UUID string `gorm:"column:uuid;primary_key" sql:"type:varchar(255) PRIMARY KEY"`

	request.Request

	Host string `sql:"size(255)"`
	Port int64  // TODO: change this UINT16

	State InstanceState

	CreatedAt time.Time
	UpdatedAt time.Time
}

// FindBaseInstance is a helper function to find the base instance.
func FindBaseInstance(brokerDb *gorm.DB, id string) (Instance, response.Response) {
	instance := Instance{}
	log.Println("Looking for base instance with id " + id)
	err := brokerDb.Where("uuid = ?", id).First(&instance).Error
	if err == nil {
		return instance, nil
	} else if err == gorm.RecordNotFound {
		return instance, response.NewErrorResponse(http.StatusNotFound, err.Error())
	} else {
		return instance, response.NewErrorResponse(http.StatusInternalServerError, err.Error())
	}
}
