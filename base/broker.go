package base

import (
	"code.cloudfoundry.org/brokerapi/v13/domain"
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
	AsyncOperationRequired(o Operation) bool
	CreateInstance(string, domain.ProvisionDetails) error
	ModifyInstance(string, domain.UpdateDetails) error
	DeleteInstance(string) error
	LastOperation(string, domain.PollDetails) (domain.LastOperation, error)
	BindInstance(string, domain.BindDetails) (domain.Binding, error)
}
