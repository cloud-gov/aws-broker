package base

import (
	"log"
	"net/http"
	"time"

	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
	"github.com/jinzhu/gorm"
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

type Instance struct {
	Uuid string `gorm:"primary_key" sql:"type:varchar(255) PRIMARY KEY"`

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
	} else if result.RecordNotFound() {
		return instance, response.NewErrorResponse(http.StatusNotFound, result.Error.Error())
	} else {
		return instance, response.NewErrorResponse(http.StatusInternalServerError, result.Error.Error())
	}
}
