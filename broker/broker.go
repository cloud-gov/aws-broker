package broker

import (
	"context"
	"errors"
	"net/http"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers/response"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"github.com/cloud-gov/aws-broker/services/elasticsearch"
	"github.com/cloud-gov/aws-broker/services/rds"
	"github.com/cloud-gov/aws-broker/services/redis"
	brokertags "github.com/cloud-gov/go-broker-tags"
	"gorm.io/gorm"
)

type AWSBroker struct {
	db         *gorm.DB
	catalog    *catalog.Catalog
	settings   *config.Settings
	jobManager *jobs.AsyncJobManager
	tagManager brokertags.TagManager
}

type CatalogExternal struct {
	Services []domain.Service `json:"services"`
}

func New(
	settings *config.Settings,
	db *gorm.DB,
	catalog *catalog.Catalog,
	jobManager *jobs.AsyncJobManager,
	tagManager brokertags.TagManager,
) *AWSBroker {
	return &AWSBroker{
		db,
		catalog,
		settings,
		jobManager,
		tagManager,
	}
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
	b.createInstance(instanceID, details, asyncAllowed)
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

func (b *AWSBroker) findBroker(serviceID string) (Broker, error) {
	switch serviceID {
	// RDS Service
	case b.catalog.RdsService.ID:
		broker, err := rds.InitRDSBroker(b.catalog, b.db, b.settings, b.tagManager)
		if err != nil {
			return nil, err
		}
		return broker, nil
	case b.catalog.RedisService.ID:
		broker, err := redis.InitRedisBroker(b.db, b.settings, b.tagManager)
		if err != nil {
			return nil, err
		}
		return broker, nil
	case b.catalog.ElasticsearchService.ID:
		broker, err := elasticsearch.InitElasticsearchBroker(b.db, b.settings, b.jobManager, b.tagManager)
		if err != nil {
			return nil, err
		}
		return broker, nil
	}

	return nil, nil
}

func (b *AWSBroker) createInstance(id string, details domain.ProvisionDetails, asyncAllowed bool) error {
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return err
	}

	// TODO: implement
	// if !asyncAllowed {
	// 	return response.ErrUnprocessableEntityResponse
	// }

	// Create instance
	resp := broker.CreateInstance(id, details)

	if resp.GetResponseType() != response.ErrorResponseType {
		instance := base.Instance{Uuid: id, Request: createRequest}
		err := b.db.Create(&instance).Error

		if err != nil {
			return response.NewErrorResponse(http.StatusBadRequest, err.Error())
		}
	}

	return resp
}
