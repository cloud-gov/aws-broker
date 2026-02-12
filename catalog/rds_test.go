package catalog

import (
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/go-test/deep"
)

func TestRDSServiceToToBrokerAPIService(t *testing.T) {
	rdsService := RDSService{
		Service: Service{
			ID:                   "id1",
			Name:                 "service1",
			Description:          "description",
			Bindable:             true,
			InstancesRetrievable: false,
			BindingsRetrievable:  false,
			Tags:                 []string{"tag1"},
			PlanUpdatable:        true,
			Requires: []domain.RequiredPermission{
				"permission1",
			},
			Metadata: &domain.ServiceMetadata{
				DisplayName: "service",
			},
			DashboardClient: &domain.ServiceDashboardClient{
				ID: "client1",
			},
			AllowContextUpdates: false,
		},
		RDSPlans: []RDSPlan{
			{
				ServicePlan: domain.ServicePlan{
					Name: "plan1",
				},
			},
		},
	}
	service := rdsService.ToBrokerAPIService()
	expectedService := domain.Service{
		ID:                   "id1",
		Name:                 "service1",
		Description:          "description",
		Bindable:             true,
		InstancesRetrievable: false,
		BindingsRetrievable:  false,
		Tags:                 []string{"tag1"},
		PlanUpdatable:        true,
		Requires: []domain.RequiredPermission{
			"permission1",
		},
		Metadata: &domain.ServiceMetadata{
			DisplayName: "service",
		},
		DashboardClient: &domain.ServiceDashboardClient{
			ID: "client1",
		},
		AllowContextUpdates: false,
		Plans: []domain.ServicePlan{
			{
				Name: "plan1",
			},
		},
	}
	if diff := deep.Equal(service, expectedService); diff != nil {
		t.Error(diff)
	}
}
