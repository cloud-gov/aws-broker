package rds

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
)

// Options is a struct containing all of the custom parameters supported by
// the broker for the "cf create-service" and "cf update-service" commands -
// they are passed in via the "-c <JSON string or file>" flag.
type Options struct {
	AllocatedStorage      int64  `json:"storage"`
	EnableFunctions       bool   `json:"enable_functions"`
	PubliclyAccessible    bool   `json:"publicly_accessible"`
	Version               string `json:"version"`
	BackupRetentionPeriod int64  `json:"backup_retention_period"`
	BinaryLogFormat       string `json:"binary_log_format"`
}

// Validate the custom parameters passed in via the "-c <JSON string or file>"
// flag that do not require checks against specific plan information.
func (o Options) Validate(settings *config.Settings) error {
	// Check to make sure that the allocated storage is less than the maximum
	// allowed.  If allocated storage is passed in, the value defaults to 0.
	if o.AllocatedStorage > settings.MaxAllocatedStorage {
		return fmt.Errorf("Invalid storage %d; must be <= %d", o.AllocatedStorage, settings.MaxAllocatedStorage)
	}

	if o.BackupRetentionPeriod > settings.MaxBackupRetention {
		return fmt.Errorf("Invalid Retention Period %d; must be <= %d", o.BackupRetentionPeriod, settings.MaxBackupRetention)
	}

	if o.BackupRetentionPeriod != 0 && o.BackupRetentionPeriod < settings.MinBackupRetention {
		return fmt.Errorf("Invalid Retention Period %d; must be => %d", o.BackupRetentionPeriod, settings.MinBackupRetention)
	}

	if err := validateBinaryLogFormat(o.BinaryLogFormat); err != nil {
		return err
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

// this helps the manager to respond appropriately depending on whether a service/plan needs an operation to be async
func (broker *rdsBroker) AsyncOperationRequired(c *catalog.Catalog, i base.Instance, o base.Operation) bool {
	switch o {
	case base.DeleteOp:
		return false
	case base.CreateOp:
		return true
	case base.ModifyOp:
		return true
	case base.BindOp:
		return false
	default:
		return false
	}
}

func (broker *rdsBroker) CreateInstance(c *catalog.Catalog, id string, createRequest request.Request) response.Response {
	newInstance := RDSInstance{}

	options := Options{}
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
	// make sure it's a valid major version.
	if options.Version != "" {
		// Check to make sure that the version specified is allowed by the plan.
		if !plan.CheckVersion(options.Version) {
			return response.NewErrorResponse(
				http.StatusBadRequest,
				options.Version+" is not a supported major version; major version must be one of: "+strings.Join(plan.ApprovedMajorVersions, ", ")+".",
			)
		}
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

func (broker *rdsBroker) ModifyInstance(c *catalog.Catalog, id string, modifyRequest request.Request, baseInstance base.Instance) response.Response {
	existingInstance := RDSInstance{}

	options := Options{}
	if len(modifyRequest.RawParameters) > 0 {
		err := json.Unmarshal(modifyRequest.RawParameters, &options)
		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
		}
	}

	// Load the existing instance provided.
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "The instance does not exist.")
	}

	// Check to see if there is a storage size change and if so, check to make sure it's a valid change.
	if options.AllocatedStorage > 0 {
		// Check that we are not decreasing the size of the instance.
		if options.AllocatedStorage < existingInstance.AllocatedStorage {
			return response.NewErrorResponse(
				http.StatusBadRequest,
				"Cannot decrease the size of an existing instance. If you need to do this, you'll need to create a new instance with the smaller size amount, backup and restore the data into that instance, and delete this instance.",
			)
		}

		// Update the existing instance with the new allocated storage.
		existingInstance.AllocatedStorage = options.AllocatedStorage
	}

	// Check if there is a backup retention change:
	if options.BackupRetentionPeriod > 0 {
		existingInstance.BackupRetentionPeriod = options.BackupRetentionPeriod
	}

	// Check if there is a binary log format change and if so, apply it
	if options.BinaryLogFormat != "" {
		existingInstance.BinaryLogFormat = options.BinaryLogFormat
	}

	// Fetch the new plan that has been requested.
	newPlan, newPlanErr := c.RdsService.FetchPlan(modifyRequest.PlanID)
	if newPlanErr != nil {
		return newPlanErr
	}

	// Check to make sure that we're not switching database engines; this is not
	// allowed.
	if newPlan.DbType != existingInstance.DbType {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot switch between database engines/types. Please select a plan with the same database engine/type.",
		)
	}

	// We shouldn't ever be able to do this as upgrades on the shared DB adapter
	// are not allowed or enabled, but in case we do, explicitly error out.
	if existingInstance.Adapter == "shared" {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot switch from a shared database instance. Please migrate your database to a dedicated instance plan instead.",
		)
	}

	// We shouldn't ever be able to do this as upgrades on the shared DB adapter
	// are not allowed or enabled, but in case we do, explicitly error out.
	if newPlan.Adapter == "shared" {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot switch to a shared database instance. Please choose a dedicated instance plan instead.",
		)
	}

	// Don't allow updating to a service plan that doesn't support updates.
	if !newPlan.PlanUpdateable {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot switch to "+newPlan.Name+" because the service plan does not allow updates or modification.",
		)
	}

	// Connect to the existing instance.
	adapter, adapterErr := initializeAdapter(newPlan, broker.settings, c)
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

	// Update the existing instance in the broker.
	existingInstance.State = status
	existingInstance.PlanID = newPlan.ID
	err = broker.brokerDB.Save(&existingInstance).Error

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, err.Error())
	}

	return response.SuccessAcceptedResponse
}

func (broker *rdsBroker) LastOperation(c *catalog.Catalog, id string, baseInstance base.Instance, operation string) response.Response {
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

func (broker *rdsBroker) BindInstance(c *catalog.Catalog, id string, bindRequest request.Request, baseInstance base.Instance) response.Response {
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
