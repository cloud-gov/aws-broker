package main

import (
	"fmt"
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

func findBroker(serviceID string, c *catalog.Catalog, brokerDb *gorm.DB, settings *config.Settings, taskqueue *taskqueue.TaskQueueManager, tagManager brokertags.TagManager) (base.Broker, response.Response) {
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

func createInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.TaskQueueManager, tagManager brokertags.TagManager) response.Response {
	createRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}
	broker, err := findBroker(createRequest.ServiceID, c, brokerDb, settings, taskqueue, tagManager)
	if err != nil {
		return err
	}

	asyncAllowed := req.FormValue("accepts_incomplete") == "true"
	if !asyncAllowed {
		return response.ErrUnprocessableEntityResponse
	}

	// Create instance
	return broker.CreateInstance(c, id, createRequest)
}

func modifyInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.TaskQueueManager, tagManager brokertags.TagManager) response.Response {
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
	broker, err := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue, tagManager)
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

func lastOperation(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.TaskQueueManager, tagManager brokertags.TagManager) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue, tagManager)
	if resp != nil {
		return resp
	}
	// pass in the operation parameter from request
	operation := req.URL.Query().Get("operation")
	fmt.Printf("lastOperation: instance %s, operation: %s\n", instance.Uuid, operation)
	return broker.LastOperation(c, id, instance, operation)
}

func bindInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.TaskQueueManager, tagManager brokertags.TagManager) response.Response {
	// Extract the request information.
	bindRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}

	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue, tagManager)
	if resp != nil {
		return resp
	}

	return broker.BindInstance(c, id, bindRequest, instance)
}

func deleteInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings, taskqueue *taskqueue.TaskQueueManager, tagManager brokertags.TagManager) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings, taskqueue, tagManager)
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
