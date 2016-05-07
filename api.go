package main

import (
	"github.com/18F/aws-broker/common/env"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/config"
	"net/http"
	"github.com/18F/aws-broker/common/context"
)

// API is a the struct to hold all the necessary data for the routes.
type API struct {
	brokerDb  *gorm.DB
	env       *env.SystemEnv
	c         *catalog.Catalog
	appConfig config.AppConfig
}

// InitAPI registers the routes for the API
func InitAPI(r *gin.RouterGroup, db *gorm.DB, env *env.SystemEnv, c *catalog.Catalog, appConfig config.AppConfig) {
	api := &API{brokerDb: db, env: env, c: c, appConfig: appConfig}
	v2 := r.Group("/v2")
	{
		v2.GET("/catalog", api.getCatalog)
		svcInstances := v2.Group("/service_instances")
		{
			svcInstances.PUT("/:instance_id", api.createInstance)
			svcInstances.DELETE("/:instance_id", api.deleteInstance)
			svcBindings := svcInstances.Group("/:instance_id/service_bindings")
			{
				svcBindings.PUT("/:binding_id", api.bindInstance)
				svcBindings.DELETE("/:binding_id", api.unbindInstance)
			}
		}
	}
}

// Serve the catalog with services and plans
func (a *API) getCatalog(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]interface{}{
		"services": a.c.GetServices(),
	})
}

// createInstance processes all requests for creating a new service instance.
// URL: /v2/service_instances/:id
// Request:
// {
//   "service_id":        "service-guid-here",
//   "plan_id":           "plan-guid-here",
//   "organization_guid": "org-guid-here",
//   "space_guid":        "space-guid-here"
// }
func (a *API) createInstance(c *gin.Context) {
	ctx := context.InitCtx(c)
	resp := createInstance(ctx, a.c, a.brokerDb, c.Param("instance_id"), a.env, a.appConfig)
	c.JSON(resp.GetStatusCode(), resp)
}

// bindInstance processes all requests for binding a service instance to an application.
// URL: /v2/service_instances/:instance_id/service_bindings/:binding_id
func (a *API) bindInstance(c *gin.Context) {
	ctx := context.InitCtx(c)
	resp := bindInstance(ctx, a.c, a.brokerDb, c.Param("instance_id"), a.env, a.appConfig)
	c.JSON(resp.GetStatusCode(), resp)
}

// unbindInstance processes all requests for unbinding a service instance from an application.
// URL: /v2/service_instances/:instance_id/service_bindings/:binding_id
func (a *API) unbindInstance(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{})
}

// deleteInstance processes all requests for deleting an existing service instance.
// URL: /v2/service_instances/:instance_id
func (a *API) deleteInstance(c *gin.Context) {
	ctx := context.InitCtx(c)
	resp := deleteInstance(ctx, a.c, a.brokerDb, c.Param("instance_id"), a.env, a.appConfig)
	c.JSON(resp.GetStatusCode(), resp)
}
