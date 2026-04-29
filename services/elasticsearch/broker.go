package elasticsearch

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"code.cloudfoundry.org/brokerapi/v13/domain/apiresponses"

	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"

	brokertags "github.com/cloud-gov/go-broker-tags"
)

type ElasticsearchAdvancedOptions struct {
	IndicesFieldDataCacheSize      string `json:"indices.fielddata.cache.size,omitempty"`
	IndicesQueryBoolMaxClauseCount string `json:"indices.query.bool.max_clause_count,omitempty"`
}

type ElasticsearchOptions struct {
	ElasticsearchVersion string                       `json:"elasticsearchVersion"`
	Bucket               string                       `json:"bucket"`
	AdvancedOptions      ElasticsearchAdvancedOptions `json:"advanced_options,omitempty"`
	VolumeType           string                       `json:"volume_type"`
}

func (o ElasticsearchOptions) Validate(settings *config.Settings) error {
	if err := validateVolumeType(o.VolumeType); err != nil {
		return err
	}
	return nil
}

type elasticsearchBroker struct {
	brokerDB   *gorm.DB
	catalog    *catalog.Catalog
	settings   *config.Settings
	logger     *slog.Logger
	tagManager brokertags.TagManager
	adapter    ElasticsearchAdapter
}

// InitelasticsearchBroker is the constructor for the elasticsearchBroker.
func InitElasticsearchBroker(
	ctx context.Context,
	catalog *catalog.Catalog,
	brokerDB *gorm.DB,
	settings *config.Settings,
	tagManager brokertags.TagManager,
	riverClient *river.Client[*sql.Tx],
	logger *slog.Logger,
) (base.Broker, error) {
	adapter, err := initializeAdapter(ctx, brokerDB, settings, logger, riverClient)
	if err != nil {
		return nil, err
	}

	return &elasticsearchBroker{
		brokerDB,
		catalog,
		settings,
		logger,
		tagManager,
		adapter,
	}, nil
}

// this helps the manager to respond appropriately depending on whether a service/plan needs an operation to be async
func (broker *elasticsearchBroker) AsyncOperationRequired(o base.Operation) bool {
	switch o {
	case base.DeleteOp:
		return true
	case base.CreateOp:
		return false
	case base.ModifyOp:
		return false
	case base.BindOp:
		return false
	default:
		return false
	}
}

func (broker *elasticsearchBroker) CreateInstance(id string, details domain.ProvisionDetails) error {
	newInstance := ElasticsearchInstance{}

	options := ElasticsearchOptions{}
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
	broker.brokerDB.Where("uuid = ?", id).First(&newInstance).Count(&count)
	if count != 0 {
		return apiresponses.ErrInstanceAlreadyExists
	}

	plan, planErr := broker.catalog.ElasticsearchService.FetchPlan(details.PlanID)
	if planErr != nil {
		return planErr
	}

	if options.ElasticsearchVersion != "" {
		// Check to make sure that the version specified is allowed by the plan.
		if !plan.CheckVersion(options.ElasticsearchVersion) {
			return apiresponses.NewFailureResponse(
				fmt.Errorf("%s is not a supported major version; major version must be one of: OpenSearch_2.3, OpenSearch_1.3, Elasticsearch_7.4", options.ElasticsearchVersion),
				http.StatusBadRequest,
				"checking Elasticsearch plan",
			)
		}
	}

	tags, err := broker.tagManager.GenerateTags(
		brokertags.Create,
		broker.catalog.ElasticsearchService.Name,
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
			fmt.Errorf("there was an error initializing the instance. Error: %s", err),
			http.StatusInternalServerError,
			"initializing instance",
		)
	}

	// Create the elasticsearch instance.
	status, err := broker.adapter.createElasticsearch(&newInstance, newInstance.ClearPassword)
	if err != nil {
		return apiresponses.NewFailureResponse(
			err,
			http.StatusInternalServerError,
			"creating instance",
		)
	}

	switch status {
	case base.InstanceNotCreated:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("error creating the instance: %s", err),
			http.StatusInternalServerError,
			"creating instance",
		)
	case base.InstanceInProgress:
		newInstance.State = status
		err = broker.brokerDB.Create(&newInstance).Error
		if err != nil {
			return apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"creating instance",
			)
		}
		return nil
	default:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("encountered unexpected state %s, error: %s", status, err),
			http.StatusInternalServerError,
			"creating instance",
		)
	}
}

func (broker *elasticsearchBroker) ModifyInstance(id string, details domain.UpdateDetails) error {

	esInstance := ElasticsearchInstance{}
	options := ElasticsearchOptions{}
	if len(details.RawParameters) > 0 {
		err := json.Unmarshal(details.RawParameters, &options)
		if err != nil {
			return apiresponses.NewFailureResponse(err, http.StatusBadRequest, "validate input parameters")
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return apiresponses.NewFailureResponse(err, http.StatusBadRequest, "validate input parameters")
		}
	}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&esInstance).Count(&count)
	if count == 0 {
		return apiresponses.ErrInstanceDoesNotExist
	}

	if esInstance.PlanID != details.PlanID {
		return apiresponses.NewFailureResponse(errors.New("Updating Elasticsearch service instances is not supported at this time."), http.StatusBadRequest, "validate input parameters")
	}

	err := esInstance.update(options)
	if err != nil {
		broker.logger.Error("Updating instance failed", "err", err)
		return apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "updating servie instance")
	}

	state, err := broker.adapter.modifyElasticsearch(&esInstance)
	if err != nil {
		broker.logger.Error("AWS call updating instance failed", "err", err)
		return apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "modifying Elasticsearch instance")
	}
	esInstance.State = state

	err = broker.brokerDB.Save(&esInstance).Error
	if err != nil {
		broker.logger.Error("Saving instance failed", "err", err)
		return apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "saving modified service instance")
	}

	return nil
}

func (broker *elasticsearchBroker) LastOperation(id string, details domain.PollDetails) (domain.LastOperation, error) {
	lastOperation := domain.LastOperation{}
	existingInstance := ElasticsearchInstance{}

	baseInstance, err := base.FindBaseInstance(broker.brokerDB, id)
	if err != nil {
		if apiErr, ok := err.(*apiresponses.FailureResponse); ok {
			if apiErr.ValidatedStatusCode(nil) == http.StatusGone && details.OperationData != base.DeleteOp.String() {
				return lastOperation, err
			}
		} else {
			return lastOperation, err
		}
	}

	var count int64
	if err := broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) || (errors.Is(err, gorm.ErrRecordNotFound) && details.OperationData != base.DeleteOp.String()) {
			return lastOperation, apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"find existing instance",
			)
		}
	}
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
	case base.DeleteOp.String():
		needAsyncJobState = broker.AsyncOperationRequired(base.DeleteOp)
		instanceOperation = base.DeleteOp
	default: //all other ops use synchronous checking of aws api
		needAsyncJobState = false
	}

	if needAsyncJobState {
		asyncJobMsg, err := asyncmessage.GetLastAsyncJobMessage(broker.brokerDB, existingInstance.ServiceID, existingInstance.Uuid, instanceOperation)
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
		instanceStatus, statusErr := broker.adapter.checkElasticsearchStatus(&existingInstance)
		if statusErr != nil {
			broker.logger.Error("Error checking Elasticsearch status", "err", statusErr)
			return lastOperation, apiresponses.NewFailureResponse(
				statusErr,
				http.StatusInternalServerError,
				"find existing instance",
			)
		}
		if err := broker.brokerDB.Save(&existingInstance).Error; err != nil {
			return lastOperation, apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"saving updated instance",
			)
		}
		state = instanceStatus
		statusMessage = fmt.Sprintf("The service instance status is %s", state)
	}

	if state == base.InstanceGone {
		broker.brokerDB.Unscoped().Delete(&existingInstance)
		broker.brokerDB.Unscoped().Delete(&baseInstance)
	}

	broker.logger.Debug(fmt.Sprintf("LastOperation - Final\n\tstate: %s\n", state))
	return domain.LastOperation{
		State:       state.ToLastOperationState(),
		Description: statusMessage,
	}, nil
}

func (broker *elasticsearchBroker) BindInstance(id string, details domain.BindDetails) (domain.Binding, error) {
	binding := domain.Binding{
		OperationData: base.BindOp.String(),
	}
	existingInstance := ElasticsearchInstance{}

	options := ElasticsearchOptions{}
	if len(details.RawParameters) > 0 {
		err := json.Unmarshal(details.RawParameters, &options)
		if err != nil {
			return binding, apiresponses.NewFailureResponse(err, http.StatusBadRequest, "validate input parameters")
		}
		err = options.Validate(broker.settings)
		if err != nil {
			return binding, apiresponses.NewFailureResponse(err, http.StatusBadRequest, "validate input parameters")
		}
	}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return binding, apiresponses.ErrInstanceDoesNotExist
	}

	password, err := existingInstance.getPassword(broker.settings.EncryptionKey)
	if err != nil {
		return binding, apiresponses.NewFailureResponse(
			fmt.Errorf("unable to get instance password: %s", err),
			http.StatusInternalServerError,
			"get instance password",
		)
	}

	// Get the correct database logic depending on the type of plan
	var credentials map[string]string
	// Bind the database instance to the application.
	existingInstance.setBucket(options.Bucket)
	if credentials, err = broker.adapter.bindElasticsearchToApp(&existingInstance, password); err != nil {
		return binding, apiresponses.NewFailureResponse(
			fmt.Errorf("there was an error binding the service to the application: %s", err),
			http.StatusInternalServerError,
			"binding service to application",
		)
	}

	binding.Credentials = credentials

	if err := broker.brokerDB.Save(&existingInstance).Error; err != nil {
		return binding, apiresponses.NewFailureResponse(
			fmt.Errorf("there was an error saving the database instance to the application: %s", err),
			http.StatusInternalServerError,
			"saving instance",
		)
	}

	return binding, nil
}

func (broker *elasticsearchBroker) DeleteInstance(id string) error {
	existingInstance := ElasticsearchInstance{}
	var count int64

	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return apiresponses.ErrInstanceDoesNotExist
	}

	password, err := existingInstance.getPassword(broker.settings.EncryptionKey)
	if err != nil {
		return apiresponses.NewFailureResponse(
			fmt.Errorf("unable to get instance password: %s", err),
			http.StatusInternalServerError,
			"get instance password",
		)
	}

	// send async deletion request.
	status, err := broker.adapter.deleteElasticsearch(&existingInstance, password)
	switch status {
	case base.InstanceGone: // somehow the instance is gone already
		broker.brokerDB.Unscoped().Delete(&existingInstance)
		return nil

	case base.InstanceInProgress: // we have done an async request
		broker.brokerDB.Save(&existingInstance)
		return nil

	default:
		broker.brokerDB.Save(&existingInstance)
		return apiresponses.NewFailureResponse(
			fmt.Errorf("error deleting the instance: %s", err),
			http.StatusInternalServerError,
			"deleting instance",
		)
	}
}
