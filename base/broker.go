package base

import (
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/common/request"
	"github.com/18F/aws-broker/common/response"
)

// Broker is the interface that every type of broker should implement.
type Broker interface {
	// CreateInstance uses the catalog and parsed request to create an instance for the particular type of service.
	CreateInstance(*catalog.Catalog, string, request.Request) response.Response
	// BindInstance takes the existing instance and binds it to an app.
	BindInstance(*catalog.Catalog, string, Instance) response.Response
	// DeleteInstance deletes the existing instance.
	DeleteInstance(*catalog.Catalog, string, Instance) response.Response
}
