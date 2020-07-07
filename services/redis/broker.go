package redis

import (
	"encoding/json"
	"net/http"

	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
)

type RedisOptions struct {
	EngineVersion string `json:"engineVersion"`
}

func (r RedisOptions) Validate(settings *config.Settings) error {
	return nil
}

type redisBroker struct {
	brokerDB *gorm.DB
	settings *config.Settings
}

// InitRedisBroker is the constructor for the redisBroker.
func InitRedisBroker(brokerDB *gorm.DB, settings *config.Settings) base.Broker {
	return &redisBroker{brokerDB, settings}
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(plan catalog.RedisPlan, s *config.Settings, c *catalog.Catalog) (redisAdapter, response.Response) {

	var redisAdapter redisAdapter

	if s.Environment == "test" {
		redisAdapter = &mockRedisAdapter{}
		return redisAdapter, nil
	}

	redisAdapter = &dedicatedRedisAdapter{
		Plan:     plan,
		settings: *s,
	}
	return redisAdapter, nil
}

func (broker *redisBroker) CreateInstance(c *catalog.Catalog, id string, createRequest request.Request) response.Response {
	newInstance := RedisInstance{}

	options := RedisOptions{}
	if len(createRequest.RawParameters) > 0 {
		err := json.Unmarshal(createRequest.RawParameters, &options)
		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
		}
	}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&newInstance).Count(&count)
	if count != 0 {
		return response.NewErrorResponse(http.StatusConflict, "The instance already exists")
	}

	plan, planErr := c.RedisService.FetchPlan(createRequest.PlanID)
	if planErr != nil {
		return planErr
	}

	err := newInstance.init(
		id,
		createRequest.OrganizationGUID,
		createRequest.SpaceGUID,
		createRequest.ServiceID,
		plan,
		options,
		broker.settings)

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "There was an error initializing the instance. Error: "+err.Error())
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}
	// Create the redis instance.
	status, err := adapter.createRedis(&newInstance, newInstance.ClearPassword)
	if status == base.InstanceNotCreated {
		desc := "There was an error creating the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}

	newInstance.State = status
	broker.brokerDB.NewRecord(newInstance)
	err = broker.brokerDB.Create(&newInstance).Error
	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, err.Error())
	}
	return response.SuccessAcceptedResponse
}

func (broker *redisBroker) ModifyInstance(c *catalog.Catalog, id string, updateRequest request.Request) response.Response {
	// Note:  This is not currently supported for Redis instances.
	return response.NewErrorResponse(http.StatusBadRequest, "Updating Redis service instances is not supported at this time.")
}

func (broker *redisBroker) LastOperation(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := RedisInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RedisService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	var state string

	status, _ := adapter.checkRedisStatus(&existingInstance)
	switch status {
	case base.InstanceInProgress:
		state = "in progress"
	case base.InstanceReady:
		state = "succeeded"
	case base.InstanceNotCreated:
		state = "failed"
	case base.InstanceNotGone:
		state = "failed"
	default:
		state = "in progress"
	}
	return response.NewSuccessLastOperation(state, "The service instance status is "+state)
}

func (broker *redisBroker) BindInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := RedisInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RedisService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	password, err := existingInstance.getPassword(broker.settings.EncryptionKey)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "Unable to get instance password.")
	}

	// Get the correct database logic depending on the type of plan. (shared vs dedicated)
	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	var credentials map[string]string
	// Bind the database instance to the application.
	originalInstanceState := existingInstance.State
	if credentials, err = adapter.bindRedisToApp(&existingInstance, password); err != nil {
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

func (broker *redisBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := RedisInstance{}
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RedisService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}
	// Delete the database instance.
	if status, err := adapter.deleteRedis(&existingInstance); status == base.InstanceNotGone {
		desc := "There was an error deleting the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}
	broker.brokerDB.Unscoped().Delete(&existingInstance)
	return response.SuccessDeleteResponse
}
