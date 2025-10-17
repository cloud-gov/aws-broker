package rds

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"code.cloudfoundry.org/brokerapi/v13/domain/apiresponses"
	"code.cloudfoundry.org/lager"

	brokertags "github.com/cloud-gov/go-broker-tags"
	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
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
	catalog    *catalog.Catalog
	settings   *config.Settings
	tagManager brokertags.TagManager
	dbAdapter  dbAdapter
}

// InitRDSBroker is the constructor for the rdsBroker.
func InitRDSBroker(catalog *catalog.Catalog, brokerDB *gorm.DB, settings *config.Settings, tagManager brokertags.TagManager) (base.BrokerV2, error) {
	logger := lager.NewLogger("aws-rds-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.INFO))
	dbAdapter, err := initializeAdapter(settings, brokerDB, logger)
	if err != nil {
		return nil, err
	}
	return &rdsBroker{brokerDB, catalog, settings, tagManager, dbAdapter}, nil
}

// this helps the manager to respond appropriately depending on whether a service/plan needs an operation to be async
func (broker *rdsBroker) AsyncOperationRequired(o base.Operation) bool {
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

func (broker *rdsBroker) CreateInstance(id string, details domain.ProvisionDetails) error {
	newInstance := NewRDSInstance()

	options := Options{}
	if len(details.RawParameters) > 0 {
		err := json.Unmarshal(details.RawParameters, &options)
		if err != nil {
			return apiresponses.ErrRawParamsInvalid
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return apiresponses.NewFailureResponse(err, http.StatusBadRequest, "validate input parameters")
		}
	}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(newInstance).Count(&count)
	if count != 0 {
		return apiresponses.ErrInstanceAlreadyExists
	}

	plan, err := broker.catalog.RdsService.FetchPlan(details.PlanID)
	if err != nil {
		return apiresponses.NewFailureResponse(err, http.StatusBadRequest, "fetching RDS plan")
	}

	// make sure it's a valid major version.
	if options.Version != "" {
		// Check to make sure that the version specified is allowed by the plan.
		if !plan.CheckVersion(options.Version) {
			return apiresponses.NewFailureResponse(
				fmt.Errorf("%s is not a supported major version; major version must be one of: %s", options.Version, strings.Join(plan.ApprovedMajorVersions, ", ")),
				http.StatusBadRequest,
				"fetching RDS plan",
			)
		}
	}

	tags, err := broker.tagManager.GenerateTags(
		brokertags.Create,
		broker.catalog.RdsService.Name,
		plan.Name,
		brokertags.ResourceGUIDs{
			InstanceGUID:     id,
			SpaceGUID:        details.SpaceGUID,
			OrganizationGUID: details.OrganizationGUID,
		},
		false,
	)
	if err != nil {
		return apiresponses.NewFailureResponse(
			err,
			http.StatusInternalServerError,
			"generating tags",
		)
	}

	err = newInstance.init(
		id,
		details.OrganizationGUID,
		details.SpaceGUID,
		details.ServiceID,
		plan,
		options,
		broker.settings,
		tags,
	)

	if err != nil {
		return apiresponses.NewFailureResponse(
			fmt.Errorf("there was an error initializing the instance. Error: %s"),
			http.StatusInternalServerError,
			"initializing instance",
		)
	}

	// Create the database instance.
	status, err := broker.dbAdapter.createDB(newInstance, plan, newInstance.ClearPassword)
	if err != nil {
		return apiresponses.NewFailureResponse(
			err,
			http.StatusInternalServerError,
			"creating RDS instance",
		)
	}

	switch status {
	case base.InstanceNotCreated:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("error creating the instance: %s", err),
			http.StatusInternalServerError,
			"creating RDS instance",
		)
	case base.InstanceInProgress:
		newInstance.State = status
		err = broker.brokerDB.Create(newInstance).Error
		if err != nil {
			return apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"creating RDS instance",
			)
		}
		return nil
	default:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("encountered unexpected state %s, error: %s", status, err),
			http.StatusInternalServerError,
			"creating RDS instance",
		)
	}
}

func (broker *rdsBroker) parseModifyOptionsFromRequest(
	details domain.UpdateDetails,
) (Options, error) {
	options := Options{}
	if len(details.RawParameters) > 0 {
		err := json.Unmarshal(details.RawParameters, &options)
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

func (broker *rdsBroker) ModifyInstance(id string, details domain.UpdateDetails) error {
	existingInstance := NewRDSInstance()

	// Load the existing instance provided.
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 {
		return apiresponses.ErrInstanceDoesNotExist
	}

	options, err := broker.parseModifyOptionsFromRequest(details)
	if err != nil {
		return apiresponses.NewFailureResponse(err, http.StatusBadRequest, "validate input parameters")
	}

	// Fetch the current plan.
	currentPlan, err := broker.catalog.RdsService.FetchPlan(existingInstance.PlanID)
	if err != nil {
		return apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "fetch RDS plan")
	}

	// Fetch the new plan that has been requested.
	newPlan, err := broker.catalog.RdsService.FetchPlan(details.PlanID)
	if err != nil {
		return apiresponses.NewFailureResponse(err, http.StatusBadRequest, "fetch RDS plan")
	}

	tags, err := broker.tagManager.GenerateTags(
		brokertags.Update,
		broker.catalog.RdsService.Name,
		newPlan.Name,
		brokertags.ResourceGUIDs{
			InstanceGUID: id,
		},
		true,
	)
	if err != nil {
		return apiresponses.NewFailureResponse(
			err,
			http.StatusInternalServerError,
			"generate tags",
		)
	}

	modifiedInstance, err := existingInstance.modify(options, currentPlan, newPlan, broker.settings, tags)
	if err != nil {
		return apiresponses.NewFailureResponse(
			fmt.Errorf("failed to modify instance. Error: %s", err),
			http.StatusInternalServerError,
			"modify RDS instance",
		)
	}

	// Check to make sure that we're not switching database engines; this is not
	// allowed.
	if newPlan.DbType != existingInstance.DbType {
		return apiresponses.NewFailureResponse(
			errors.New("cannot switch between database engines/types. Please select a plan with the same database engine/type"),
			http.StatusBadRequest,
			"modify RDS instance",
		)
	}

	// Don't allow updating to a service plan that doesn't support updates.
	if !newPlan.PlanUpdateable {
		return apiresponses.ErrPlanChangeNotSupported
	}

	// Modify the database instance.
	status, err := broker.dbAdapter.modifyDB(modifiedInstance, newPlan)

	switch status {
	case base.InstanceNotModified:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("error modifying the instance: %s", err),
			http.StatusInternalServerError,
			"modify RDS instance",
		)
	case base.InstanceInProgress:
		// Update the existing instance in the broker.
		existingInstance.State = status
		err = broker.brokerDB.Save(existingInstance).Error
		if err != nil {
			return apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"modify RDS instance",
			)
		}
		return nil
	default:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("encountered unexpected state %s, error: %s", status, err),
			http.StatusInternalServerError,
			"modify RDS instance",
		)
	}
}

func (broker *rdsBroker) LastOperation(id string, details domain.PollDetails) (domain.LastOperation, error) {
	lastOperation := domain.LastOperation{}
	existingInstance := NewRDSInstance()

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 && details.OperationData != base.DeleteOp.String() {
		return lastOperation, apiresponses.ErrInstanceDoesNotExist
	}

	// When asynchronous deletion has finished, the instance record no longer exists, so
	// return a last operation status indicating that the deletion was successful.
	if count == 0 && details.OperationData == base.DeleteOp.String() {
		return domain.LastOperation{
			State:       domain.Succeeded,
			Description: "Successfully deleted instance",
		}, nil
	}

	var state base.InstanceState
	var needAsyncJobState bool
	var instanceOperation base.Operation
	var statusMessage string

	switch details.OperationData {
	case base.CreateOp.String():
		needAsyncJobState = broker.AsyncOperationRequired(base.CreateOp)
		instanceOperation = base.CreateOp
	case base.ModifyOp.String():
		needAsyncJobState = broker.AsyncOperationRequired(base.ModifyOp)
		instanceOperation = base.ModifyOp
	case base.DeleteOp.String():
		needAsyncJobState = broker.AsyncOperationRequired(base.DeleteOp)
		instanceOperation = base.DeleteOp
	default:
		needAsyncJobState = false
	}

	if needAsyncJobState {
		asyncJobMsg, err := jobs.GetLastAsyncJobMessage(broker.brokerDB, existingInstance.ServiceID, existingInstance.Uuid, instanceOperation)
		if err != nil {
			return lastOperation, apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"get last async job message",
			)
		}
		state = asyncJobMsg.JobState.State
		statusMessage = asyncJobMsg.JobState.Message
	} else {
		dbState, err := broker.dbAdapter.checkDBStatus(existingInstance.Database)
		if err != nil {
			return lastOperation, apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"check DB status",
			)
		}
		state = dbState
		statusMessage = fmt.Sprintf("The database status is %s", state)
	}

	return domain.LastOperation{
		State:       state.ToLastOperationState(),
		Description: statusMessage,
	}, nil
}

func (broker *rdsBroker) BindInstance(id string, details domain.BindDetails) (domain.Binding, error) {
	binding := domain.Binding{}

	existingInstance := NewRDSInstance()

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 {
		return binding, apiresponses.ErrInstanceDoesNotExist
	}

	password, err := existingInstance.dbUtils.getPassword(
		existingInstance.Salt,
		existingInstance.Password,
		broker.settings.EncryptionKey,
	)
	if err != nil {
		return binding, apiresponses.NewFailureResponse(
			fmt.Errorf("unable to get instance password: %s", err),
			http.StatusInternalServerError,
			"get instance password",
		)
	}

	var credentials map[string]string
	// Bind the database instance to the application.
	originalInstanceState := existingInstance.State
	if credentials, err = broker.dbAdapter.bindDBToApp(existingInstance, password); err != nil {
		return binding, apiresponses.NewFailureResponse(
			fmt.Errorf("there was an error binding the database instance to the application: %s", err),
			http.StatusInternalServerError,
			"get instance password",
		)
	}

	// If the state of the instance has changed, update it.
	if existingInstance.State != originalInstanceState {
		broker.brokerDB.Save(existingInstance)
	}

	return domain.Binding{
		Credentials: credentials,
	}, nil
}

func (broker *rdsBroker) DeleteInstance(id string) error {
	existingInstance := NewRDSInstance()
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(existingInstance).Count(&count)
	if count == 0 {
		return apiresponses.ErrInstanceDoesNotExist
	}

	// Delete the database instance.
	status, err := broker.dbAdapter.deleteDB(existingInstance)
	if err != nil {
		return apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "delete RDS instance")
	}

	switch status {
	case base.InstanceNotGone:
		return apiresponses.NewFailureResponse(fmt.Errorf("error deleting the instance: %s", err), http.StatusInternalServerError, "delete RDS instance")
	case base.InstanceInProgress:
		return nil
	default:
		return apiresponses.NewFailureResponse(fmt.Errorf("encountered unexpected state %s, error: %s", status, err), http.StatusInternalServerError, "delete RDS instance")
	}
}
