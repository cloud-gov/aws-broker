package main

import (
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/taskqueue"
	"github.com/go-martini/martini"
	"github.com/jinzhu/gorm"
	"github.com/martini-contrib/render"

	"net/http"

	"github.com/cloud-gov/aws-broker/catalog"
)

/*
type Operation struct {
	State                    string
	Description              string
	AsyncPollIntervalSeconds int `json:"async_poll_interval_seconds, omitempty"`
}

type CreateResponse struct {
	DashboardUrl  string
	LastOperation Operation
}
*/

// CreateInstance processes all requests for creating a new service instance.
// URL: /v2/service_instances/:id
// Request:
//
//	{
//	  "service_id":        "service-guid-here",
//	  "plan_id":           "plan-guid-here",
//	  "organization_guid": "org-guid-here",
//	  "space_guid":        "space-guid-here"
//	}
func CreateInstance(p martini.Params, req *http.Request, r render.Render, brokerDb *gorm.DB, s *config.Settings, c *catalog.Catalog, q *taskqueue.QueueManager) {
	resp := createInstance(req, c, brokerDb, p["id"], s, q)
	r.JSON(resp.GetStatusCode(), resp)
}

// ModifyInstance processes all requests for modifying an existing service instance.
// URL: /v2/service_instances/:id
// Request:
//
//	{
//	  "service_id":        "service-guid-here",
//	  "plan_id":           "plan-guid-here",
//	  "organization_guid": "org-guid-here",
//	  "space_guid":        "space-guid-here"
//	}
func ModifyInstance(p martini.Params, req *http.Request, r render.Render, brokerDb *gorm.DB, s *config.Settings, c *catalog.Catalog, q *taskqueue.QueueManager) {
	resp := modifyInstance(req, c, brokerDb, p["id"], s, q)
	r.JSON(resp.GetStatusCode(), resp)
}

// LastOperation processes all requests for binding a service instance to an application.
// URL: /v2/service_instances/:instance_id/last_operation
func LastOperation(p martini.Params, req *http.Request, r render.Render, brokerDb *gorm.DB, s *config.Settings, c *catalog.Catalog, q *taskqueue.QueueManager) {
	resp := lastOperation(req, c, brokerDb, p["instance_id"], s, q)
	r.JSON(resp.GetStatusCode(), resp)
}

// BindInstance processes all requests for binding a service instance to an application.
// URL: /v2/service_instances/:instance_id/service_bindings/:binding_id
func BindInstance(p martini.Params, req *http.Request, r render.Render, brokerDb *gorm.DB, s *config.Settings, c *catalog.Catalog, q *taskqueue.QueueManager) {
	resp := bindInstance(req, c, brokerDb, p["instance_id"], s, q)
	r.JSON(resp.GetStatusCode(), resp)
}

// DeleteInstance processes all requests for deleting an existing service instance.
// URL: /v2/service_instances/:instance_id
func DeleteInstance(p martini.Params, req *http.Request, r render.Render, brokerDb *gorm.DB, s *config.Settings, c *catalog.Catalog, q *taskqueue.QueueManager) {
	resp := deleteInstance(req, c, brokerDb, p["instance_id"], s, q)
	r.JSON(resp.GetStatusCode(), resp)
}
