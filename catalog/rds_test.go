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

func TestRDSPlanCheckVersion(t *testing.T) {
	plan := RDSPlan{
		ApprovedMajorVersions: []string{"8.4"},
	}
	if plan.CheckVersion("8.4.9") != true {
		t.Fatal("specifying minor version for major version should return true")
	}

	plan = RDSPlan{
		ApprovedMajorVersions: []string{"8.4"},
	}
	if plan.CheckVersion("8.4") != true {
		t.Fatal("specifying minor version should return true")
	}

	plan = RDSPlan{
		ApprovedMajorVersions: []string{"15", "16"},
	}
	if plan.CheckVersion("15.3") != true {
		t.Fatal("specifying minor version for major version should return true")
	}

	plan = RDSPlan{
		ApprovedMajorVersions: []string{"15", "16"},
	}
	if plan.CheckVersion("17") != false {
		t.Fatal("version should not be approved")
	}
	if plan.CheckVersion("17.3") != false {
		t.Fatal("version should not be approved")
	}
}
