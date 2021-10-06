package base

import (
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
)

// Broker is the interface that every type of broker should implement.
type Broker interface {
	// CreateInstance uses the catalog and parsed request to create an instance for the particular type of service.
	CreateInstance(*catalog.Catalog, string, request.Request) response.Response
	// ModifyInstance uses the catalog and parsed request to modify an existing instance for the particular type of service.
	ModifyInstance(*catalog.Catalog, string, request.Request, Instance) response.Response
	// LastOperation uses the catalog and parsed request to get an instance status for the particular type of service.
	LastOperation(*catalog.Catalog, string, Instance) response.Response
	// BindInstance takes the existing instance and binds it to an app.
	BindInstance(*catalog.Catalog, string, request.Request, Instance) response.Response
	// DeleteInstance deletes the existing instance.
	DeleteInstance(*catalog.Catalog, string, Instance) response.Response
}
