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

const (
	createOpString = "create"
	modifyOpString = "modify"
	deleteOpString = "delete"
	bindOpString   = "bind"
	unbindOpString = "unbind"
)

func (o Operation) String() string {
	switch o {
	case CreateOp:
		return createOpString
	case ModifyOp:
		return modifyOpString
	case DeleteOp:
		return deleteOpString
	case BindOp:
		return bindOpString
	case UnBindOp:
		return unbindOpString
	default:
		return "unknown"
	}
}

var operationStringToConstantMap = map[string]Operation{
	createOpString: CreateOp,
	modifyOpString: ModifyOp,
	deleteOpString: DeleteOp,
	bindOpString:   BindOp,
	unbindOpString: UnBindOp,
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

func ConvertOperationStringToConstant(operation string) Operation {
	return operationStringToConstantMap[operation]
}
