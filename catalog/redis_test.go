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

	// Plans provision Valkey, so an empty engine resolves to Valkey.
	if plan.Engine != "valkey" {
		t.Errorf("Expected plan engine to be valkey, got %q", plan.Engine)
	}

	testCases := map[string]struct {
		version string
		engine  string
		valid   bool
	}{
		// Valid versions for the plan engine, with the engine left implicit.
		"8.2 on plan engine":            {version: "8.2", valid: true},
		"9.1 on plan engine":            {version: "9.1", valid: true},
		"redis version on plan engine":  {version: "7.1", valid: false},
		"8.2 on valkey":                 {version: "8.2", engine: "valkey", valid: true},
		"9.1 on valkey":                 {version: "9.1", engine: "valkey", valid: true},
		"redis version on valkey":       {version: "7.0", engine: "valkey", valid: false},
		"unsupported version on valkey": {version: "7.9", engine: "valkey", valid: false},
		// Redis versions stay approved so pre-existing Redis instances can
		// still be updated in place.
		"7.0 on redis":                 {version: "7.0", engine: "redis", valid: true},
		"7.1 on redis":                 {version: "7.1", engine: "redis", valid: true},
		"valkey version on redis":      {version: "8.2", engine: "redis", valid: false},
		"unsupported version on redis": {version: "6.2", engine: "redis", valid: false},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			if got := plan.CheckVersion(test.version, test.engine); got != test.valid {
				t.Errorf(
					"CheckVersion(%q, %q) = %t, want %t",
					test.version, test.engine, got, test.valid,
				)
			}
		})
	}
}
