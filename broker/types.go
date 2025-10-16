package broker

import "code.cloudfoundry.org/brokerapi/v13/domain"

// Broker is the interface that every type of broker should implement.
type Broker interface {
	// CreateInstance uses the catalog and parsed request to create an instance for the particular type of service.
	CreateInstance(string, domain.ProvisionDetails) error
}
