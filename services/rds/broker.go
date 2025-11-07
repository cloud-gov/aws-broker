package rds

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"code.cloudfoundry.org/lager"

	brokertags "github.com/cloud-gov/go-broker-tags"
	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/helpers/response"
	jobs "github.com/cloud-gov/aws-broker/jobs"
)

// Options is a struct containing all of the custom parameters supported by
// the broker for the "cf create-service" and "cf update-service" commands -
// they are passed in via the "-c <JSON string or file>" flag.
type Options struct {
	AllocatedStorage                int64    `json:"storage"`
	EnableFunctions                 bool     `json:"enable_functions"`
	PubliclyAccessible              bool     `json:"publicly_accessible"`
	Version                         string   `json:"version"`
	BackupRetentionPeriod           *int64   `json:"backup_retention_period"`
	BinaryLogFormat                 string   `json:"binary_log_format"`
	EnablePgCron                    *bool    `json:"enable_pg_cron"`
	RotateCredentials               *bool    `json:"rotate_credentials"`
	StorageType                     string   `json:"storage_type"`
	EnableCloudWatchLogGroupExports []string `json:"enable_cloudwatch_log_groups_exports"`
}

// Validate the custom parameters passed in via the "-c <JSON string or file>"
// flag that do not require checks against specific plan information.
func (o Options) Validate(settings *config.Settings) error {
	// Check to make sure that the allocated storage is less than the maximum
	// allowed.  If allocated storage is passed in, the value defaults to 0.
	if o.AllocatedStorage > settings.MaxAllocatedStorage {
		return fmt.Errorf("invalid storage %d; must be <= %d", o.AllocatedStorage, settings.MaxAllocatedStorage)
	}

	if o.BackupRetentionPeriod != nil && *o.BackupRetentionPeriod > settings.MaxBackupRetention {
		return fmt.Errorf("invalid Retention Period %d; must be <= %d", o.BackupRetentionPeriod, settings.MaxBackupRetention)
	}

	if o.BackupRetentionPeriod != nil && *o.BackupRetentionPeriod < settings.MinBackupRetention {
		return fmt.Errorf("invalid Retention Period %d; must be => %d", o.BackupRetentionPeriod, settings.MinBackupRetention)
	}

	if err := validateBinaryLogFormat(o.BinaryLogFormat); err != nil {
		return err
	}

	if err := validateStorageType(o.StorageType); err != nil {
		return err
	}

	return nil
}

type rdsBroker struct {
	brokerDB   *gorm.DB
	settings   *config.Settings
	tagManager brokertags.TagManager
	dbAdapter  dbAdapter
}

// InitRDSBroker is the constructor for the rdsBroker.
func InitRDSBroker(brokerDB *gorm.DB, settings *config.Settings, tagManager brokertags.TagManager) (base.Broker, error) {
	logger := lager.NewLogger("aws-rds-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.INFO))
	dbAdapter, err := initializeAdapter(settings, brokerDB, logger)
	if err != nil {
		return nil, err
	}
	return &rdsBroker{brokerDB, settings, tagManager, dbAdapter}, nil
}

// this helps the manager to respond appropriately depending on whether a service/plan needs an operation to be async
func (broker *rdsBroker) AsyncOperationRequired(c *catalog.Catalog, i base.Instance, o base.Operation) bool {
	switch o {
	case base.DeleteOp:
		return true
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
	newInstance := NewRDSInstance()

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
	broker.brokerDB.Where("uuid = ?", id).First(newInstance).Count(&count)
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

	tags, err := broker.tagManager.GenerateTags(
		brokertags.Create,
		c.RdsService.Name,
		plan.Name,
		brokertags.ResourceGUIDs{
			InstanceGUID:     id,
			SpaceGUID:        createRequest.SpaceGUID,
			OrganizationGUID: createRequest.OrganizationGUID,
		},
		false,
	)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "There was an error generating the tags. Error: "+err.Error())
	}

	err = newInstance.init(
		id,
		createRequest.OrganizationGUID,
		createRequest.SpaceGUID,
		createRequest.ServiceID,
		plan,
		options,
		broker.settings,
		tags,
	)

	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "There was an error initializing the instance. Error: "+err.Error())
	}

	// Create the database instance.
	status, err := broker.dbAdapter.createDB(newInstance, plan, newInstance.ClearPassword)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
	}

	switch status {
	case base.InstanceNotCreated:
		return response.NewErrorResponse(http.StatusInternalServerError, fmt.Sprintf("Error creating the instance: %s", err))
	case base.InstanceInProgress:
		newInstance.State = status
		err = broker.brokerDB.Create(newInstance).Error
		if err != nil {
			return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		return response.NewAsyncOperationResponse(base.CreateOp.String())
	default:
		return response.NewErrorResponse(http.StatusInternalServerError, fmt.Sprintf("Encountered unexpected state %s, error: %s", status, err))
	}
}

func (broker *rdsBroker) parseModifyOptionsFromRequest(
	modifyRequest request.Request,
) (Options, error) {
	options := Options{}
	if len(modifyRequest.RawParameters) > 0 {
		err := json.Unmarshal(modifyRequest.RawParameters, &options)
		if err != nil {
			return options, err
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return options, err
		}
	}
	return options, nil
}

func (broker *rdsBroker) ModifyInstance(c *catalog.Catalog, id string, modifyRequest request.Request, baseInstance base.Instance) response.Response {
	existingInstance := NewRDSInstance()

	// Load the existing instance provided.
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "The instance does not exist.")
	}

	options, err := broker.parseModifyOptionsFromRequest(modifyRequest)
	if err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, "Invalid parameters. Error: "+err.Error())
	}

	// Fetch the current plan.
	currentPlan, errResponse := c.RdsService.FetchPlan(existingInstance.PlanID)
	if errResponse != nil {
		return errResponse
	}

	// Fetch the new plan that has been requested.
	newPlan, errResponse := c.RdsService.FetchPlan(modifyRequest.PlanID)
	if errResponse != nil {
		return errResponse
	}

	tags, err := broker.tagManager.GenerateTags(
		brokertags.Update,
		c.RdsService.Name,
		newPlan.Name,
		brokertags.ResourceGUIDs{
			InstanceGUID:     id,
			SpaceGUID:        modifyRequest.SpaceGUID,
			OrganizationGUID: modifyRequest.OrganizationGUID,
		},
		true,
	)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "There was an error generating the tags. Error: "+err.Error())
	}

	modifiedInstance, err := existingInstance.modify(options, currentPlan, newPlan, broker.settings, tags)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "Failed to modify instance. Error: "+err.Error())
	}

	// Check to make sure that we're not switching database engines; this is not
	// allowed.
	if newPlan.DbType != existingInstance.DbType {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot switch between database engines/types. Please select a plan with the same database engine/type.",
		)
	}

	// Don't allow updating to a service plan that doesn't support updates.
	if !newPlan.PlanUpdateable {
		return response.NewErrorResponse(
			http.StatusBadRequest,
			"Cannot switch to "+newPlan.Name+" because the service plan does not allow updates or modification.",
		)
	}

	// Modify the database instance.
	status, err := broker.dbAdapter.modifyDB(modifiedInstance, newPlan)

	switch status {
	case base.InstanceNotModified:
		return response.NewErrorResponse(http.StatusInternalServerError, fmt.Sprintf("Error modifying the instance: %s", err))
	case base.InstanceInProgress:
		// Update the existing instance in the broker.
		err = broker.brokerDB.Model(RDSInstance{}).Where("uuid", existingInstance.Uuid).Update("state", status).Error
		if err != nil {
			return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		return response.NewAsyncOperationResponse(base.ModifyOp.String())
	default:
		return response.NewErrorResponse(http.StatusInternalServerError, fmt.Sprintf("Encountered unexpected state %s, error: %s", status, err))
	}
}

func (broker *rdsBroker) LastOperation(c *catalog.Catalog, id string, baseInstance base.Instance, operation string) response.Response {
	existingInstance := NewRDSInstance()

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 && operation != base.DeleteOp.String() {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	// When asynchronous deletion has finished, the instance record no longer exists, so
	// return a last operation status indicating that the deletion was successful.
	if count == 0 && operation == base.DeleteOp.String() {
		return response.NewSuccessLastOperation(base.InstanceGone.ToLastOperationStatus(), "Successfully deleted instance")
	}

	var state base.InstanceState
	var needAsyncJobState bool
	var instanceOperation base.Operation
	var statusMessage string

	switch operation {
	case base.CreateOp.String():
		// creation always uses an async job
		needAsyncJobState = true
		instanceOperation = base.CreateOp
	case base.ModifyOp.String():
		// modify always uses an async job
		needAsyncJobState = true
		instanceOperation = base.ModifyOp
	case base.DeleteOp.String():
		// deletion always uses an async job
		needAsyncJobState = true
		instanceOperation = base.DeleteOp
	default:
		needAsyncJobState = false
	}

	if needAsyncJobState {
		asyncJobMsg, err := jobs.GetLastAsyncJobMessage(broker.brokerDB, existingInstance.ServiceID, existingInstance.Uuid, instanceOperation)
		if err != nil {
			return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		state = asyncJobMsg.JobState.State
		statusMessage = asyncJobMsg.JobState.Message
	} else {
		dbState, err := broker.dbAdapter.checkDBStatus(existingInstance.Database)
		if err != nil {
			return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		state = dbState
		statusMessage = fmt.Sprintf("The database status is %s", state)
	}

	return response.NewSuccessLastOperation(state.ToLastOperationStatus(), statusMessage)
}

func (broker *rdsBroker) BindInstance(c *catalog.Catalog, id string, bindRequest request.Request, baseInstance base.Instance) response.Response {
	existingInstance := NewRDSInstance()

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	password, err := existingInstance.dbUtils.getPassword(
		existingInstance.Salt,
		existingInstance.Password,
		broker.settings.EncryptionKey,
	)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "Unable to get instance password.")
	}

	var credentials map[string]string
	// Bind the database instance to the application.
	originalInstanceState := existingInstance.State
	if credentials, err = broker.dbAdapter.bindDBToApp(existingInstance, password); err != nil {
		return response.NewErrorResponse(http.StatusBadRequest, fmt.Sprintf("There was an error binding the database instance to the application. Error: %s", err))
	}

	// If the state of the instance has changed, update it.
	if existingInstance.State != originalInstanceState {
		broker.brokerDB.Save(existingInstance)
	}

	return response.NewSuccessBindResponse(credentials)
}

func (broker *rdsBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := NewRDSInstance()
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	// Delete the database instance.
	status, err := broker.dbAdapter.deleteDB(existingInstance)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, err.Error())
	}

	switch status {
	case base.InstanceNotGone:
		return response.NewErrorResponse(http.StatusInternalServerError, fmt.Sprintf("Error deleting the instance: %s", err))
	case base.InstanceInProgress:
		return response.NewAsyncOperationResponse(base.DeleteOp.String())
	default:
		return response.NewErrorResponse(http.StatusInternalServerError, fmt.Sprintf("Encountered unexpected state %s, error: %s", status, err))
	}
}
