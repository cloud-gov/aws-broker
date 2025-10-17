package base

import (
	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/cloud-gov/aws-broker/helpers/response"
)

// operation represents the type of async operation a broker may require
type Operation uint8

const (
	NoOp Operation = iota
	CreateOp
	ModifyOp
	DeleteOp
	BindOp
	UnBindOp
)

func (o Operation) String() string {
	switch o {
	case CreateOp:
		return "create"
	case ModifyOp:
		return "modify"
	case DeleteOp:
		return "delete"
	case BindOp:
		return "bind"
	case UnBindOp:
		return "unbind"
	default:
		return "unknown"
	}
}

// Broker is the interface that every type of broker should implement.
type Broker interface {
	// CreateInstance uses the catalog and parsed request to create an instance for the particular type of service.
	CreateInstance(*catalog.Catalog, string, request.Request) response.Response
	// ModifyInstance uses the catalog and parsed request to modify an existing instance for the particular type of service.
	ModifyInstance(*catalog.Catalog, string, request.Request, Instance) response.Response
	// LastOperation uses the catalog and parsed request to get an instance status for the particular type of service.
	LastOperation(*catalog.Catalog, string, Instance, string) response.Response
	// BindInstance takes the existing instance and binds it to an app.
	BindInstance(*catalog.Catalog, string, request.Request, Instance) response.Response
	// DeleteInstance deletes the existing instance.
	DeleteInstance(*catalog.Catalog, string, Instance) response.Response
	// Supports Async operation
	AsyncOperationRequired(*catalog.Catalog, Instance, Operation) bool
}

type BrokerV2 interface {
	AsyncOperationRequired(o Operation) bool
	CreateInstance(string, domain.ProvisionDetails) error
	ModifyInstance(string, domain.UpdateDetails) error
	DeleteInstance(string) error
}
