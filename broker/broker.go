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
	return b.catalog.GetServices(), nil
}

func (b *AWSBroker) Provision(
	context context.Context,
	instanceID string,
	details domain.ProvisionDetails,
	asyncAllowed bool,
) (domain.ProvisionedServiceSpec, error) {
	return b.createInstance(instanceID, details, asyncAllowed)
}

func (b *AWSBroker) Update(
	context context.Context,
	instanceID string,
	details domain.UpdateDetails,
	asyncAllowed bool,
) (domain.UpdateServiceSpec, error) {
	return b.modifyInstance(instanceID, details, asyncAllowed)
}

func (b *AWSBroker) Deprovision(
	context context.Context,
	instanceID string,
	details domain.DeprovisionDetails,
	asyncAllowed bool,
) (domain.DeprovisionServiceSpec, error) {
	return b.deleteInstance(instanceID, details, asyncAllowed)
}

func (b *AWSBroker) Bind(
	context context.Context,
	instanceID string,
	bindingID string,
	details domain.BindDetails,
	asyncAllowed bool,
) (domain.Binding, error) {
	return b.bindInstance(instanceID, details, asyncAllowed)
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
	return b.lastOperation(instanceID, details)
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
		broker, err := redis.InitRedisBroker(b.catalog, b.db, b.settings, b.tagManager)
		if err != nil {
			return nil, err
		}
		return broker, nil
	case b.catalog.ElasticsearchService.ID:
		broker, err := elasticsearch.InitElasticsearchBroker(b.catalog, b.db, b.settings, b.jobManager, b.tagManager)
		if err != nil {
			return nil, err
		}
		return broker, nil
	}

	return nil, nil
}

func (b *AWSBroker) createInstance(id string, details domain.ProvisionDetails, asyncAllowed bool) (domain.ProvisionedServiceSpec, error) {
	spec := domain.ProvisionedServiceSpec{}
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return spec, err
	}

	asyncRequired := broker.AsyncOperationRequired(base.CreateOp)
	spec.IsAsync = asyncRequired

	if asyncRequired && !asyncAllowed {
		return spec, apiresponses.ErrAsyncRequired
	}

	// Create instance
	err = broker.CreateInstance(id, details)
	if err != nil {
		return spec, apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "create instance")
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
		return spec, apiresponses.NewFailureResponse(err, http.StatusBadRequest, "save new instance")
	}

	return spec, nil
}

func (b *AWSBroker) modifyInstance(id string, details domain.UpdateDetails, asyncAllowed bool) (domain.UpdateServiceSpec, error) {
	spec := domain.UpdateServiceSpec{}
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return spec, err
	}

	instance, err := base.FindBaseInstance(b.db, id)
	if err != nil {
		return spec, err
	}

	asyncRequired := broker.AsyncOperationRequired(base.ModifyOp)
	spec.IsAsync = asyncRequired

	if asyncRequired && !asyncAllowed {
		return spec, apiresponses.ErrAsyncRequired
	}

	// Attempt to modify the database instance.
	err = broker.ModifyInstance(id, details)
	if err == nil {
		err := b.db.Save(&instance).Error
		if err != nil {
			return spec, apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"save updated instance",
			)
		}
	}

	return spec, nil
}

func (b *AWSBroker) deleteInstance(id string, details domain.DeprovisionDetails, asyncAllowed bool) (domain.DeprovisionServiceSpec, error) {
	spec := domain.DeprovisionServiceSpec{}
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return spec, err
	}

	asyncRequired := broker.AsyncOperationRequired(base.DeleteOp)
	spec.IsAsync = asyncRequired

	if asyncRequired && !asyncAllowed {
		return spec, apiresponses.ErrAsyncRequired
	}

	// Create instance
	err = broker.DeleteInstance(id)
	if err != nil {
		return spec, apiresponses.NewFailureResponse(err, http.StatusInternalServerError, "delete instance")
	}

	instance, err := base.FindBaseInstance(b.db, id)
	if err != nil {
		return spec, err
	}

	err = broker.DeleteInstance(id)

	// only delete from DB if it was a sync delete and succeeded
	if err == nil && !asyncRequired {
		err := b.db.Unscoped().Delete(&instance).Error
		if err != nil {
			return spec, apiresponses.NewFailureResponse(
				err,
				http.StatusInternalServerError,
				"delete instance",
			)
		}
	}

	return spec, nil
}

func (b *AWSBroker) bindInstance(id string, details domain.BindDetails, asyncAllowed bool) (domain.Binding, error) {
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return domain.Binding{}, err
	}

	asyncRequired := broker.AsyncOperationRequired(base.BindOp)
	if asyncRequired && !asyncAllowed {
		return domain.Binding{}, apiresponses.ErrAsyncRequired
	}

	return broker.BindInstance(id, details)
}

func (b *AWSBroker) lastOperation(id string, details domain.PollDetails) (domain.LastOperation, error) {
	broker, err := b.findBroker(details.ServiceID)
	if err != nil {
		return domain.LastOperation{}, err
	}

	return broker.LastOperation(id, details)
}
