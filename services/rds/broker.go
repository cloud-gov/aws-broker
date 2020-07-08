package rds

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
)

type RDSOptions struct {
	AllocatedStorage   int64 `json:"storage"`
	EnableFunctions    bool  `json:"enable_functions"`
	PubliclyAccessible bool  `json:"publicly_accessible"`
}

func (r RDSOptions) Validate(settings *config.Settings) error {
	if r.AllocatedStorage > settings.MaxAllocatedStorage {
		return fmt.Errorf("Invalid storage %d; must be <= %d", r.AllocatedStorage, settings.MaxAllocatedStorage)
	}
	return nil
}

type rdsBroker struct {
	brokerDB *gorm.DB
	settings *config.Settings
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(plan catalog.RDSPlan, s *config.Settings, c *catalog.Catalog) (dbAdapter, response.Response) {

	var dbAdapter dbAdapter
	// For test environments, use a mock adapter.
	if s.Environment == "test" {
		dbAdapter = &mockDBAdapter{}
		return dbAdapter, nil
	}

	switch plan.Adapter {
	case "shared":
		setting, err := c.GetResources().RdsSettings.GetRDSSettingByPlan(plan.ID)
		if err != nil {
			return nil, response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		if setting.DB == nil {
			return nil, response.NewErrorResponse(http.StatusInternalServerError, "An internal error occurred setting up shared databases.")
		}
		dbAdapter = &sharedDBAdapter{
			SharedDbConn: setting.DB,
		}
	case "dedicated":
		dbAdapter = &dedicatedDBAdapter{
			Plan:     plan,
			settings: *s,
		}
	default:
		return nil, response.NewErrorResponse(http.StatusInternalServerError, "Adapter not found")
	}

	return dbAdapter, nil
}

// InitRDSBroker is the constructor for the rdsBroker.
func InitRDSBroker(brokerDB *gorm.DB, settings *config.Settings) base.Broker {
	return &rdsBroker{brokerDB, settings}
}

func (broker *rdsBroker) CreateInstance(c *catalog.Catalog, id string, createRequest request.Request) response.Response {
	newInstance := RDSInstance{}

	options := RDSOptions{}
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
		options,
		broker.settings)

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "There was an error initializing the instance. Error: "+err.Error())
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}
	// Create the database instance.
	status, err := adapter.createDB(&newInstance, newInstance.ClearPassword)
	if status == base.InstanceNotCreated {
		desc := "There was an error creating the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}

	newInstance.State = status

	if newInstance.Adapter == "shared" {
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
	return response.SuccessAcceptedResponse
}

func (broker *rdsBroker) ModifyInstance(c *catalog.Catalog, id string, updateRequest request.Request) response.Response {
	existingInstance := RDSInstance{}

	options := RDSOptions{}
	// TODO: Figure out how these parameter checks work and if anything needs to be modified
	if len(updateRequest.RawParameters) > 0 {
		err := json.Unmarshal(updateRequest.RawParameters, &options)
		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
		}
	}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count != 1 {
		return response.NewErrorResponse(http.StatusConflict, "The instance does not exist")
	}

	plan, planErr := c.RdsService.FetchPlan(updateRequest.PlanID)
	if planErr != nil {
		return planErr
	}

	err := existingInstance.init(
		id,
		updateRequest.OrganizationGUID,
		updateRequest.SpaceGUID,
		updateRequest.ServiceID,
		plan,
		options,
		broker.settings)

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "There was an error initializing the instance. Error: "+err.Error())
	}

	// We shouldn't ever arrive to this as upgrades on the shared DB adapter are
	// not allowed or enabled, but in case we do, explicitly error out.
	if existingInstance.Adapter == "shared" {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot update a shared database instance. Please migrate to a dedicated instance plan instead.",
		)
	}

	// Don't allow updating to a service plan that doesn't support updates.
	// TODO:  Is this the right way to perform this check, and is this the right
	//		  way to reference a plan name that you would supply as an argument
	//		  (e.g., "micro-psql")?
	if plan.PlanUpdateable == false {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"You cannot change your service instance to the plan you requested, "+plan.Name+"; it is not supported.",
		)
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	// Modify the database instance.
	status, err := adapter.modifyDB(&existingInstance, existingInstance.ClearPassword)

	if status == base.InstanceNotModified {
		desc := "There was an error modifying the instance."

		if err != nil {
			desc = desc + " Error: " + err.Error()
		}

		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}

	existingInstance.State = status

	// TODO:  Check to make sure this is not needed.
	// broker.brokerDB.NewRecord(existingInstance)
	// TODO:  Check to make sure this is the correct call to make.
	err = broker.brokerDB.Update(&existingInstance).Error

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	return response.SuccessAcceptedResponse
}

func (broker *rdsBroker) LastOperation(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := RDSInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RdsService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	var state string
	status, _ := adapter.checkDBStatus(&existingInstance)
	switch status {
	case base.InstanceInProgress:
		state = "in progress"
	case base.InstanceReady:
		state = "succeeded"
	case base.InstanceNotCreated:
		state = "failed"
	case base.InstanceNotModified:
		state = "failed"
	case base.InstanceNotGone:
		state = "failed"
	default:
		state = "in progress"
	}
	return response.NewSuccessLastOperation(state, "The service instance status is "+state)
}

func (broker *rdsBroker) BindInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := RDSInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RdsService.FetchPlan(baseInstance.PlanID)
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
	if credentials, err = adapter.bindDBToApp(&existingInstance, password); err != nil {
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

func (broker *rdsBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := RDSInstance{}
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.RdsService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}
	// Delete the database instance.
	if status, err := adapter.deleteDB(&existingInstance); status == base.InstanceNotGone {
		desc := "There was an error deleting the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}
	broker.brokerDB.Unscoped().Delete(&existingInstance)
	return response.SuccessDeleteResponse
}
