package catalog

import (
	"path/filepath"
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/go-test/deep"
)

func TestRedisServiceToBrokerAPIService(t *testing.T) {
	redisService := RedisService{
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
		RedisPlans: []RedisPlan{
			{
				ServicePlan: domain.ServicePlan{
					Name: "plan1",
				},
			},
		},
	}
	service := redisService.ToBrokerAPIService()
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

func TestRedisCheckVersion(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)
	redisTestPlanID := "475e36bf-387f-44c1-9b81-575fec2ee443"

	plan, err := catalog.RedisService.FetchPlan(redisTestPlanID)
	if err != nil {
		t.Error("Could not fetch plan " + redisTestPlanID)
	}

	// Test that a valid version for the current engine returns true.
	validVersion := plan.CheckVersion("7.1", "")
	if !validVersion {
		t.Error("Valid version check failed.")
	}

	// Test that an invalid version for the current engine returns false.
	validVersion = plan.CheckVersion("8.2", "")
	if validVersion {
		t.Error("Invalid version check failed.")
	}

	// Test that a valid version for a new engine returns true.
	validVersion = plan.CheckVersion("8.2", "valkey")
	if !validVersion {
		t.Error("Valid version check failed.")
	}

	// Test that an invalid version for a new engine returns false.
	validVersion = plan.CheckVersion("7.0", "valkey")
	if validVersion {
		t.Error("Invalid version check failed.")
	}
}
