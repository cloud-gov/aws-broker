package main

import (
	"net/http"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
	"github.com/18F/aws-broker/services/elasticsearch"
	"github.com/18F/aws-broker/services/rds"
	"github.com/18F/aws-broker/services/redis"
	"github.com/jinzhu/gorm"
)

func findBroker(serviceID string, c *catalog.Catalog, brokerDb *gorm.DB, settings *config.Settings) (base.Broker, response.Response) {
	switch serviceID {
	// RDS Service
	case c.RdsService.ID:
		return rds.InitRDSBroker(brokerDb, settings), nil
	case c.RedisService.ID:
		return redis.InitRedisBroker(brokerDb, settings), nil
	case c.ElasticsearchService.ID:
		return elasticsearch.InitElasticsearchBroker(brokerDb, settings), nil
	}

	return nil, response.NewErrorResponse(http.StatusNotFound, catalog.ErrNoServiceFound.Error())
}

func createInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings) response.Response {
	createRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}
	broker, err := findBroker(createRequest.ServiceID, c, brokerDb, settings)
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

func modifyInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings) response.Response {
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
	broker, err := findBroker(instance.ServiceID, c, brokerDb, settings)
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

func lastOperation(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings)
	if resp != nil {
		return resp
	}

	return broker.LastOperation(c, id, instance)
}

func bindInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings) response.Response {
	// Extract the request information.
	bindRequest, err := request.ExtractRequest(req)
	if err != nil {
		return err
	}

	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings)
	if resp != nil {
		return resp
	}

	return broker.BindInstance(c, id, bindRequest, instance)
}

func deleteInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, settings *config.Settings) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, settings)
	if resp != nil {
		return resp
	}

	resp = broker.DeleteInstance(c, id, instance)
	if resp.GetResponseType() != response.ErrorResponseType {
		brokerDb.Unscoped().Delete(&instance)
		// TODO check delete error
	}
	return resp
}
