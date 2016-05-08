package sqs

import (
	"net/http"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
	"github.com/jinzhu/gorm"
)

type sqsBroker struct {
	brokerDB *gorm.DB
	settings *config.Settings
}

// InitSQSBroker is the constructor for the sqsBroker.
func InitSQSBroker(brokerDB *gorm.DB, settings *config.Settings) base.Broker {
	return &sqsBroker{brokerDB, settings}
}

func (broker *sqsBroker) CreateInstance(c *catalog.Catalog, id string, createRequest request.Request) response.Response {
	newInstance := SQSInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&newInstance).Count(&count)
	if count != 0 {
		return response.NewErrorResponse(http.StatusConflict, "The instance already exists")
	}

	plan, planErr := c.Services[1].FetchPlan(createRequest.PlanID)
	if planErr != nil {
		return planErr
	}

	err := newInstance.init(
		id,
		createRequest.OrganizationGUID,
		createRequest.SpaceGUID,
		createRequest.ServiceID,
		plan,
		broker.settings)

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "There was an error initializing the instance. Error: "+err.Error())
	}

	return response.SuccessCreateResponse
}

func (broker *sqsBroker) BindInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := SQSInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	return nil
}

func (broker *sqsBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	return nil
}
