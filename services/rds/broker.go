package rds

import (
	"net/http"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
	"github.com/jinzhu/gorm"
)

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

	switch plan.Properties.Adapter {
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
			InstanceClass: plan.Properties.InstanceClass,
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
	//newInstance := RDSInstance{}
	/*
		var count int64
		broker.brokerDB.Where("uuid = ?", id).First(&newInstance).Count(&count)
		if count != 0 {
			return response.NewErrorResponse(http.StatusConflict, "The instance already exists")
		}

		plan, planErr := c.Services[0].FetchPlan(createRequest.PlanID)
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
	*/
	return response.SuccessCreateResponse

}

func (broker *rdsBroker) BindInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	//existingInstance := RDSInstance{}
	/*
		var count int64
		broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
		if count == 0 {
			return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
		}

		plan, planErr := c.Services[0].FetchPlan(baseInstance.PlanID)
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
	*/
	var credentials map[string]string
	return response.NewSuccessBindResponse(credentials)

}

func (broker *rdsBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	/*
		existingInstance := RDSInstance{}
		var count int64
		broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
		if count == 0 {
			return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
		}

		plan, planErr := c.Services[0].FetchPlan(baseInstance.PlanID)
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
	*/
	return response.SuccessDeleteResponse

}
