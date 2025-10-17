package broker

import (
	"context"
	"errors"
	"net/http"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"code.cloudfoundry.org/brokerapi/v13/domain/apiresponses"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers/request"
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
	asyncRequired, err := b.createInstance(instanceID, details, asyncAllowed)
	if err != nil {
		return domain.ProvisionedServiceSpec{}, err
	}
	return domain.ProvisionedServiceSpec{IsAsync: asyncRequired}, nil
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

func (b *AWSBroker) findBroker(serviceID string) (base.BrokerV2, error) {
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

func (b *AWSBroker) createInstance(id string, details domain.ProvisionDetails, asyncAllowed bool) (bool, error) {
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return false, err
	}

	asyncRequired := broker.AsyncOperationRequired(base.CreateOp)
	if broker.AsyncOperationRequired(base.CreateOp) && !asyncAllowed {
		return asyncRequired, apiresponses.ErrAsyncRequired
	}

	// Create instance
	err = broker.CreateInstance(id, details)
	if err != nil {
		return asyncRequired, apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "create instance")
	}

	instance := base.Instance{Uuid: id, Request: request.Request{
		ServiceID:        details.ServiceID,
		PlanID:           details.PlanID,
		OrganizationGUID: details.OrganizationGUID,
		SpaceGUID:        details.SpaceGUID,
		RawParameters:    details.RawParameters,
	}}

	err = b.db.Create(&instance).Error
	if err != nil {
		return asyncRequired, apiresponses.NewFailureResponse(err, http.StatusBadRequest, "save new instance")
	}

	return asyncRequired, nil
}
