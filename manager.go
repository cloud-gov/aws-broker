package main

import (
	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/config"
	"github.com/18F/aws-broker/common/env"
	"github.com/18F/aws-broker/common/request"
	"github.com/18F/aws-broker/common/response"
	"github.com/18F/aws-broker/services/rds"
	"github.com/jinzhu/gorm"
	"net/http"
)

func findBroker(serviceID string, c *catalog.Catalog, brokerDb *gorm.DB,
	env *env.SystemEnv, appConfig config.AppConfig) (base.Broker, response.Response) {
	switch serviceID {
	// RDS Service
	case c.RdsService.ID:
		return rds.InitRDSBroker(brokerDb, env, appConfig.DBAdapter), nil
	}

	return nil, response.NewErrorResponse(http.StatusNotFound, catalog.ErrNoServiceFound.Error())
}

func createInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB,
	id string, env *env.SystemEnv, appConfig config.AppConfig) response.Response {
	createRequest, resp := request.ExtractRequest(req)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(createRequest.ServiceID, c, brokerDb, env, appConfig)
	if resp != nil {
		return resp
	}

	// Create instance
	resp = broker.CreateInstance(c, id, createRequest)
	if resp.GetResponseType() != response.ErrorResponseType {
		instance := base.Instance{UUID: id, Request: createRequest}
		brokerDb.NewRecord(instance)
		brokerDb.Create(&instance)
		// TODO check save error
	}
	return resp
}

func bindInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB,
	id string, env *env.SystemEnv, appConfig config.AppConfig) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, env, appConfig)
	if resp != nil {
		return resp
	}

	return broker.BindInstance(c, id, instance)
}

func deleteInstance(req *http.Request, c *catalog.Catalog, brokerDb *gorm.DB, id string, env *env.SystemEnv, appConfig config.AppConfig) response.Response {
	instance, resp := base.FindBaseInstance(brokerDb, id)
	if resp != nil {
		return resp
	}
	broker, resp := findBroker(instance.ServiceID, c, brokerDb, env, appConfig)
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
