package broker

import (
	"context"
	"errors"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	brokertags "github.com/cloud-gov/go-broker-tags"
)

type AWSBroker struct {
}

type CatalogExternal struct {
	Services []domain.Service `json:"services"`
}

func New(
	tagManager brokertags.TagManager,
) *AWSBroker {
	return &AWSBroker{}
}

func (b *AWSBroker) Services(context context.Context) ([]domain.Service, error) {
	return []domain.Service{}, nil
}

func (b *AWSBroker) Provision(
	context context.Context,
	instanceID string,
	details domain.ProvisionDetails,
	asyncAllowed bool,
) (domain.ProvisionedServiceSpec, error) {
	return domain.ProvisionedServiceSpec{IsAsync: false}, nil
}

func (b *AWSBroker) Update(
	context context.Context,
	instanceID string,
	details domain.UpdateDetails,
	asyncAllowed bool,
) (domain.UpdateServiceSpec, error) {
	return domain.UpdateServiceSpec{IsAsync: false}, nil
}

func (b *AWSBroker) Deprovision(
	context context.Context,
	instanceID string,
	details domain.DeprovisionDetails,
	asyncAllowed bool,
) (domain.DeprovisionServiceSpec, error) {
	return domain.DeprovisionServiceSpec{IsAsync: false}, nil
}

func (b *AWSBroker) Bind(
	context context.Context,
	instanceID string,
	bindingID string,
	details domain.BindDetails,
	asyncAllowed bool,
) (domain.Binding, error) {

	return domain.Binding{}, nil
}

func (b *AWSBroker) Unbind(
	context context.Context,
	instanceID,
	bindingID string,
	details domain.UnbindDetails,
	asyncAllowed bool,
) (domain.UnbindSpec, error) {
	return domain.UnbindSpec{}, nil
}

func (b *AWSBroker) LastOperation(
	ctx context.Context,
	instanceID string,
	details domain.PollDetails,
) (domain.LastOperation, error) {
	return domain.LastOperation{}, nil
}

func (b *AWSBroker) GetBinding(
	ctx context.Context,
	instanceID,
	bindingID string,
	details domain.FetchBindingDetails,
) (domain.GetBindingSpec, error) {
	return domain.GetBindingSpec{}, errors.New("this broker does not support GetBinding")
}

func (b *AWSBroker) GetInstance(
	ctx context.Context,
	instanceID string,
	details domain.FetchInstanceDetails,
) (domain.GetInstanceDetailsSpec, error) {
	return domain.GetInstanceDetailsSpec{}, errors.New("this broker does not support GetInstance")
}

func (b *AWSBroker) LastBindingOperation(
	ctx context.Context,
	instanceID,
	bindingID string,
	details domain.PollDetails,
) (domain.LastOperation, error) {
	return domain.LastOperation{}, errors.New("this broker does not support LastBindingOperation")
}
