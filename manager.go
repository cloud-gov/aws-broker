package main

import (
	"net/http"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/helpers/response"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	"github.com/cloud-gov/aws-broker/services/rds"
	"github.com/cloud-gov/aws-broker/services/redis"
	"github.com/cloud-gov/aws-broker/taskqueue"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"github.com/jinzhu/gorm"
)

type mockTagGenerator struct {
	tags map[string]string
}

func (mt *mockTagGenerator) GenerateTags(
	action brokertags.Action,
	serviceName string,
	servicePlanName string,
	resourceGUIDs brokertags.ResourceGUIDs,
	getMissingResources bool,
) (map[string]string, error) {
	return mt.tags, nil
}

func findBroker(serviceID string, c *catalog.Catalog, brokerDb *gorm.DB, settings *config.Settings, taskqueue *taskqueue.QueueManager) (base.Broker, response.Response) {
	var tagManager brokertags.TagManager
	if settings.Environment == "test" {
		tagManager = &mockTagGenerator{}
	} else {
		var err error
		tagManager, err = brokertags.NewCFTagManager(
			"AWS broker",
			settings.Environment,
			settings.CfApiUrl,
			settings.CfApiClientId,
			settings.CfApiClientSecret,
		)
		if err != nil {
			return nil, response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
	}

	switch serviceID {
	// RDS Service
	case c.RdsService.ID:
		return rds.InitRDSBroker(brokerDb, settings, tagManager), nil
	case c.RedisService.ID:
		return redis.InitRedisBroker(brokerDb, settings, tagManager), nil
	case c.ElasticsearchService.ID:
		broker, err := elasticsearch.InitElasticsearchBroker(brokerDb, settings, taskqueue, tagManager)
		if err != nil {
			return nil, response.NewErrorResponse(http.StatusInternalServerError, err.Error())
		}
		return broker, nil
	}

	return nil, response.NewErrorResponse(http.StatusNotFound, catalog.ErrNoServiceFound.Error())
}

func createInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.QueueManager) response.Response {
	createRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}
	broker, err := findBroker(createRequest.ServiceID, c, brokerDb, settings, taskqueue)
	if err != nil {
		return err
	}

	asyncAllowed := req.FormValue("accepts_incomplete") == "true"
	if !asyncAllowed {
		return response.ErrUnprocessableEntityResponse
	}

	// Create instance
	resp := broker.CreateInstance(c, id, createRequest)

	if resp.GetResponseType() != response.ErrorResponseType {
		instance := base.Instance{Uuid: id, Request: createRequest}
		brokerDb.NewRecord(instance)

		err := brokerDb.Create(&instance).Error

		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, err.Error())
		}
	}

	return resp
}

func modifyInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.QueueManager) response.Response {
	// Extract the request information.
	modifyRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}

	// Find the requested instance in the broker.
	instance, err := base.FindBaseInstance(brokerDb, id)
	if err != nil {
		return err
	}

	// Retrieve the correct broker.
	broker, err := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue)
	if err != nil {
		return err
	}

	// Check if async calls are allowed.
	asyncAllowed := req.FormValue("accepts_incomplete") == "true"
	if !asyncAllowed {
		return response.ErrUnprocessableEntityResponse
	}

	// Attempt to modify the database instance.
	resp := broker.ModifyInstance(c, id, modifyRequest, instance)

	if resp.GetResponseType() != response.ErrorResponseType {
		err := brokerDb.Save(&instance).Error

		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, err.Error())
		}
	}

	return resp
}

func lastOperation(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.QueueManager) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue)
	if resp != nil {
		return resp
	}
	// pass in the operation parameter from request
	operation := req.URL.Query().Get("operation")
	return broker.LastOperation(c, id, instance, operation)
}

func bindInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.QueueManager) response.Response {
	// Extract the request information.
	bindRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}

	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue)
	if resp != nil {
		return resp
	}

	return broker.BindInstance(c, id, bindRequest, instance)
}

func deleteInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.QueueManager) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue)
	if resp != nil {
		return resp
	}
	if broker.AsyncOperationRequired(c, instance, base.DeleteOp) {
		// Check if async calls are allowed.
		asyncAllowed := req.FormValue("accepts_incomplete") == "true"
		if !asyncAllowed {
			return response.ErrUnprocessableEntityResponse
		}
	}
	resp = broker.DeleteInstance(c, id, instance)
	//only delete from DB if it was a sync delete and succeeded
	if resp.GetResponseType() == response.SuccessDeleteResponseType {
		brokerDb.Unscoped().Delete(&instance)
		// TODO check delete error
	}
	return resp
}
