package elasticsearch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"code.cloudfoundry.org/lager"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
)

type ElasticsearchAdvancedOptions struct {
	IndicesFieldDataCacheSize      string `json:"indices.fielddata.cache.size,omitempty"`
	IndicesQueryBoolMaxClauseCount string `json:"indices.query.bool.max_clause_count,omitempty"`
}

type ElasticsearchOptions struct {
	ElasticsearchVersion string                       `json:"elasticsearchVersion"`
	Bucket               string                       `json:"bucket"`
	AdvancedOptions      ElasticsearchAdvancedOptions `json:"advanced_options,omitempty"`
}

func (r ElasticsearchOptions) Validate(settings *config.Settings) error {
	return nil
}

type elasticsearchBroker struct {
	brokerDB *gorm.DB
	settings *config.Settings
}

// InitelasticsearchBroker is the constructor for the elasticsearchBroker.
func InitElasticsearchBroker(brokerDB *gorm.DB, settings *config.Settings) base.Broker {
	return &elasticsearchBroker{brokerDB, settings}
}

// initializeAdapter is the main function to create database instances
func initializeAdapter(plan catalog.ElasticsearchPlan, s *config.Settings, c *catalog.Catalog) (ElasticsearchAdapter, response.Response) {

	var elasticsearchAdapter ElasticsearchAdapter
	if s.Environment == "test" {
		elasticsearchAdapter = &mockElasticsearchAdapter{}
		return elasticsearchAdapter, nil
	}

	elasticsearchAdapter = &dedicatedElasticsearchAdapter{
		Plan:     plan,
		settings: *s,
	}
	return elasticsearchAdapter, nil
}

// this helps the manager to respond appropriately depending on whether a service/plan needs an operation to be async
func (broker *elasticsearchBroker) AsyncOperationRequired(c *catalog.Catalog, i base.Instance, o base.Operation) bool {
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

func (broker *elasticsearchBroker) CreateInstance(c *catalog.Catalog, id string, createRequest request.Request) response.Response {
	newInstance := ElasticsearchInstance{}

	options := ElasticsearchOptions{}
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

	plan, planErr := c.ElasticsearchService.FetchPlan(createRequest.PlanID)
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
	// Create the elasticsearch instance.
	status, err := adapter.createElasticsearch(&newInstance, newInstance.ClearPassword)
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
	return response.NewAsyncOperationResponse(base.CreateOp.String())
}

func (broker *elasticsearchBroker) ModifyInstance(c *catalog.Catalog, id string, updateRequest request.Request, baseInstance base.Instance) response.Response {
	logger := lager.NewLogger("aws-broker")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))
	esInstance := ElasticsearchInstance{}
	options := ElasticsearchOptions{}
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

	plan, planErr := c.ElasticsearchService.FetchPlan(updateRequest.PlanID)
	if planErr != nil {
		return planErr
	}
	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&esInstance).Count(&count)
	if count != 1 {
		return response.NewErrorResponse(http.StatusNotFound, "The instance doesn't exist")
	}
	if esInstance.PlanID != updateRequest.PlanID {
		return response.NewErrorResponse(http.StatusBadRequest, "Updating Elasticsearch service instances is not supported at this time.")
	}
	err := esInstance.update(options)
	if err != nil {
		logger.Error("Updating instance failed", err)
		return response.NewErrorResponse(http.StatusBadRequest, "Error updating Elasticsearch service instance")
	}
	err = broker.brokerDB.Save(&esInstance).Error
	if err != nil {
		logger.Error("Saving instance failed", err)
		return response.NewErrorResponse(http.StatusBadRequest, "Error updating Elasticsearch service instance")
	}

	_, err = adapter.modifyElasticsearch(&esInstance, esInstance.ClearPassword)
	if err != nil {
		logger.Error("AWS call updating instance failed", err)
		return response.NewErrorResponse(http.StatusBadRequest, "Error updating Elasticsearch service instance")
	}
	return response.NewAsyncOperationResponse(base.ModifyOp.String())
}

func (broker *elasticsearchBroker) LastOperation(c *catalog.Catalog, id string, baseInstance base.Instance, operation string) response.Response {
	existingInstance := ElasticsearchInstance{}

	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.ElasticsearchService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	var state string
	// two async ops -- create and delete
	status, _ := adapter.checkElasticsearchStatus(&existingInstance, operation)
	switch status {
	case base.InstanceInProgress:
		state = "in progress"
	case base.InstanceReady:
		state = "succeeded"
	case base.InstanceNotCreated:
		state = "failed"
	case base.InstanceGone:
		state = "succeeded"
		broker.brokerDB.Unscoped().Delete(&existingInstance)
		broker.brokerDB.Unscoped().Delete(&baseInstance)
	case base.InstanceNotGone:
		state = "failed"
	default:
		state = "in progress"
	}
	fmt.Printf("LastOperation - Final\n\tstate: %s", state)
	return response.NewSuccessLastOperation(state, "The service instance status is "+state)
}

func (broker *elasticsearchBroker) BindInstance(c *catalog.Catalog, id string, bindRequest request.Request, baseInstance base.Instance) response.Response {
	existingInstance := ElasticsearchInstance{}

	options := ElasticsearchOptions{}
	if len(bindRequest.RawParameters) > 0 {
		err := json.Unmarshal(bindRequest.RawParameters, &options)
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
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.ElasticsearchService.FetchPlan(baseInstance.PlanID)
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
	existingInstance.setBucket(options.Bucket)
	if credentials, err = adapter.bindElasticsearchToApp(&existingInstance, password); err != nil {
		desc := "There was an error binding the database instance to the application."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}

	if len(existingInstance.Bucket) > 0 {
		broker.brokerDB.Save(&existingInstance)
	}
	// If the state of the instance has changed, update it.
	if existingInstance.State != originalInstanceState {
		broker.brokerDB.Save(&existingInstance)
	}

	return response.NewSuccessBindResponse(credentials)
}

func (broker *elasticsearchBroker) DeleteInstance(c *catalog.Catalog, id string, baseInstance base.Instance) response.Response {
	existingInstance := ElasticsearchInstance{}
	var count int64
	broker.brokerDB.Where("uuid = ?", id).First(&existingInstance).Count(&count)
	if count == 0 {
		return response.NewErrorResponse(http.StatusNotFound, "Instance not found")
	}

	plan, planErr := c.ElasticsearchService.FetchPlan(baseInstance.PlanID)
	if planErr != nil {
		return planErr
	}
	password, err := existingInstance.getPassword(broker.settings.EncryptionKey)
	if err != nil {
		return response.NewErrorResponse(http.StatusInternalServerError, "Unable to get instance password.")
	}

	adapter, adapterErr := initializeAdapter(plan, broker.settings, c)
	if adapterErr != nil {
		return adapterErr
	}

	// send async deletion request.
	status, err := adapter.deleteElasticsearch(&existingInstance, password, broker.brokerDB)
	switch status {
	case base.InstanceGone: // somehow the instance is gone already
		broker.brokerDB.Unscoped().Delete(&existingInstance)
		broker.brokerDB.Unscoped().Delete(&baseInstance)
		return response.SuccessDeleteResponse

	case base.InstanceInProgress: // we have done an async request
		broker.brokerDB.Save(&existingInstance)
		return response.NewAsyncOperationResponse(base.DeleteOp.String())
	default:
		desc := "There was an error deleting the instance."
		if err != nil {
			desc = desc + " Error: " + err.Error()
		}
		return response.NewErrorResponse(http.StatusBadRequest, desc)
	}
	// if status != base.InstanceInProgress {
	// 	desc := "There was an error deleting the instance."
	// 	if err != nil {
	// 		desc = desc + " Error: " + err.Error()
	// 	}
	// 	return response.NewErrorResponse(http.StatusBadRequest, desc)
	// }
	// // save the state for polling
	// broker.brokerDB.Save(&existingInstance)
	// return response.NewAsyncOperationResponse(base.DeleteOp.String())
	// // we need make this an async cleanup when base.InstanceGone state is set.
	// //broker.brokerDB.Unscoped().Delete(&existingInstance)

}
