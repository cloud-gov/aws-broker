package redis

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"code.cloudfoundry.org/brokerapi/v13/domain/apiresponses"
	"code.cloudfoundry.org/lager"

	brokertags "github.com/cloud-gov/go-broker-tags"
	"gorm.io/gorm"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
)

type RedisOptions struct {
	EngineVersion string `json:"engineVersion"`
}

func (r RedisOptions) Validate(settings *config.Settings) error {
	return nil
}

type redisBroker struct {
	brokerDB   *gorm.DB
	catalog    *catalog.Catalog
	settings   *config.Settings
	logger     lager.Logger
	tagManager brokertags.TagManager
	adapter    redisAdapter
}

// InitRedisBroker is the constructor for the redisBroker.
func InitRedisBroker(
	catalog *catalog.Catalog,
	brokerDB *gorm.DB,
	settings *config.Settings,
	tagManager brokertags.TagManager,
) (base.BrokerV2, error) {
	logger := lager.NewLogger("aws-redis-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.INFO))

	adapter, err := initializeAdapter(settings, logger)
	if err != nil {
		return nil, err
	}

	return &redisBroker{brokerDB, catalog, settings, logger, tagManager, adapter}, nil
}

// this helps the manager to respond appropriately depending on whether a service/plan needs an operation to be async
func (broker *redisBroker) AsyncOperationRequired(o base.Operation) bool {
	switch o {
	case base.DeleteOp:
		return false
	case base.CreateOp:
		return true
	case base.ModifyOp:
		return false
	case base.BindOp:
		return false
	default:
		return false
	}
}

func (broker *redisBroker) CreateInstance(id string, details domain.ProvisionDetails) error {
	newInstance := RedisInstance{}

	options := RedisOptions{}
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

	plan, planErr := broker.catalog.RedisService.FetchPlan(details.PlanID)
	if planErr != nil {
		return planErr
	}
	if options.EngineVersion != "" {
		// Check to make sure that the version specified is allowed by the plan.
		if !plan.CheckVersion(options.EngineVersion) {
			return apiresponses.NewFailureResponse(
				fmt.Errorf(options.EngineVersion+" is not a supported major version; major version must be one of: 7.0, 6.2, 6.0, 5.0.6"),
				http.StatusBadRequest,
				"checking Redis plan",
			)
		}
	}

	tags, err := broker.tagManager.GenerateTags(
		brokertags.Create,
		broker.catalog.RedisService.Name,
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

	// Create the redis instance.
	status, err := broker.adapter.createRedis(&newInstance, newInstance.ClearPassword)
	if err != nil {
		return apiresponses.NewFailureResponse(
			err,
			http.StatusInternalServerError,
			"creating Redis instance",
		)
	}

	switch status {
	case base.InstanceNotCreated:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("error creating the instance: %s", err),
			http.StatusInternalServerError,
			"creating Redis instance",
		)
	case base.InstanceInProgress:
		newInstance.State = status
		err = broker.brokerDB.Create(&newInstance).Error
		if err != nil {
			return apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"creating Redis instance",
			)
		}
		return nil
	default:
		return apiresponses.NewFailureResponse(
			fmt.Errorf("encountered unexpected state %s, error: %s", status, err),
			http.StatusInternalServerError,
			"creating Redis instance",
		)
	}
}

func (broker *redisBroker) ModifyInstance(id string, details domain.UpdateDetails) error {
	// Note:  This is not currently supported for Redis instances.
	return apiresponses.NewFailureResponse(
		fmt.Errorf("Updating Redis service instances is not supported at this time."),
		http.StatusBadRequest,
		"modifying Redis instance",
	)
}

func (broker *redisBroker) LastOperation(id string, details domain.PollDetails) (domain.LastOperation, error) {
	lastOperation := domain.LastOperation{}
	existingInstance := RedisInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 && details.OperationData != base.DeleteOp.String() {
		return lastOperation, apiresponses.ErrInstanceDoesNotExist
	}

	status, err := broker.adapter.checkRedisStatus(&existingInstance)
	if err != nil {
		broker.logger.Error("Error checking Redis status", err)
		return lastOperation, apiresponses.NewFailureResponse(
			err,
			http.StatusInternalServerError,
			"check Redis status",
		)
	}

	return domain.LastOperation{
		State:       status.ToLastOperationState(),
		Description: fmt.Sprintf("The service instance status is %s", status),
	}, nil
}

func (broker *redisBroker) BindInstance(id string, details domain.BindDetails) (domain.Binding, error) {
	binding := domain.Binding{}
	existingInstance := RedisInstance{}

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

	var credentials map[string]string
	// Bind the database instance to the application.
	originalInstanceState := existingInstance.State
	if credentials, err = broker.adapter.bindRedisToApp(&existingInstance, password); err != nil {
		return binding, apiresponses.NewFailureResponse(
			fmt.Errorf("there was an error binding the instance to the application: %s", err),
			http.StatusInternalServerError,
			"get instance password",
		)
	}

	// If the state of the instance has changed, update it.
	if existingInstance.State != originalInstanceState {
		if err := broker.brokerDB.Save(&existingInstance).Error; err != nil {
			return binding, apiresponses.NewFailureResponse(
				fmt.Errorf("there was an error saving the instance to the application: %s", err),
				http.StatusInternalServerError,
				"saving instance",
			)
		}
	}

	return domain.Binding{
		Credentials: credentials,
	}, nil
}

func (broker *redisBroker) DeleteInstance(id string) error {
	existingInstance := RedisInstance{}
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return apiresponses.ErrInstanceDoesNotExist
	}

	// Delete the database instance.
	status, err := broker.adapter.deleteRedis(&existingInstance)
	if err != nil {
		return apiresponses.NewFailureResponse(
			fmt.Errorf("there was an error deleting the instance: %s", err),
			http.StatusInternalServerError,
			"delete instance",
		)
	}

	switch status {
	case base.InstanceNotGone:
		return apiresponses.NewFailureResponse(fmt.Errorf("error deleting the instance: %s", err), http.StatusInternalServerError, "delete Redis instance")
	case base.InstanceGone:
		err := broker.brokerDB.Unscoped().Delete(&existingInstance).Error
		if err != nil {
			return apiresponses.NewFailureResponse(fmt.Errorf("error deleting the instance: %s", err), http.StatusInternalServerError, "delete Redis instance")
		}
		return nil
	default:
		return apiresponses.NewFailureResponse(fmt.Errorf("encountered unexpected state %s, error: %s", status, err), http.StatusInternalServerError, "delete Redis instance")
	}
}
