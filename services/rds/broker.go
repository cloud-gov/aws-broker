package rds

import (
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/env"
	"github.com/18F/aws-broker/common/request"
	"github.com/18F/aws-broker/common/response"
	"github.com/jinzhu/gorm"
	"net/http"
"github.com/18F/aws-broker/common/context"
)

type rdsBroker struct {
	brokerDB *gorm.DB
	env      *env.SystemEnv
	adapter  DBAdapter
}

// InitRDSBroker is the constructor for the rdsBroker.
func InitRDSBroker(brokerDB *gorm.DB, env *env.SystemEnv, dbAdapter DBAdapter, ctx context.Ctx) base.Broker {
	return &rdsBroker{brokerDB: brokerDB, env: env, adapter: dbAdapter}
}

func (broker *rdsBroker) CreateInstance(c *catalog.Catalog, id string,
	createRequest request.Request, ctx context.Ctx) response.Response {
	newInstance := Instance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&newInstance).Count(&count)
	if count != 0 {
		return response.NewErrorResponse(http.StatusConflict, "The instance already exists")
	}

	plan, planErr := c.RdsService.FetchPlan(createRequest.PlanID)
	if planErr != nil {
		return planErr
	}

	err := newInstance.init(
		id,
		createRequest.OrganizationGUID,
		createRequest.SpaceGUID,
		createRequest.ServiceID,
		plan,
		broker.env)

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "There was an error initializing the instance. Error: "+err.Error())
	}

	agent, agentErr := broker.adapter.findBrokerAgent(plan, c)
	if agentErr != nil {
		return agentErr
	}
	// Create the database instance.
	status, err := agent.createDB(&newInstance, newInstance.ClearPassword)
	if status == base.InstanceNotCreated {
		desc := "There was an error creating the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}

	newInstance.State = status

	if newInstance.Agent == "shared" {
		setting, err := c.GetResources().RdsSettings.GetRDSSettingByPlan(plan.ID)
		if err != nil {
			return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		newInstance.Host = setting.Config.URL
		newInstance.Port = setting.Config.Port
	}
	broker.brokerDB.NewRecord(newInstance)
	err = broker.brokerDB.Create(&newInstance).Error
	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, err.Error())
	}
	return response.SuccessCreateResponse
}

func (broker *rdsBroker) BindInstance(c *catalog.Catalog, id string, baseInstance base.Instance, ctx context.Ctx) response.Response {
	existingInstance := Instance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RdsService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	password, err := existingInstance.getPassword(broker.env.EncryptionKey)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "Unable to get instance password.")
	}

	// Get the correct database logic depending on the type of plan. (shared vs dedicated)
	agent, agentErr := broker.adapter.findBrokerAgent(plan, c)
	if agentErr != nil {
		return agentErr
	}

	var credentials map[string]string
	// Bind the database instance to the application.
	originalInstanceState := existingInstance.State
	if credentials, err = agent.bindDBToApp(&existingInstance, password); err != nil {
		desc := "There was an error binding the database instance to the application."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}

	// If the state of the instance has changed, update it.
	if existingInstance.State != originalInstanceState {
		broker.brokerDB.Save(&existingInstance)
	}

	return response.NewSuccessBindResponse(credentials)
}

func (broker *rdsBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance, ctx context.Ctx) response.Response {
	existingInstance := Instance{}
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RdsService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	agent, agentErr := broker.adapter.findBrokerAgent(plan, c)
	if agentErr != nil {
		return agentErr
	}
	// Delete the database instance.
	if status, err := agent.deleteDB(&existingInstance); status == base.InstanceNotGone {
		desc := "There was an error deleting the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}
	broker.brokerDB.Unscoped().Delete(&existingInstance)
	return response.SuccessDeleteResponse
}
