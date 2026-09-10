package catalog

import (
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/go-test/deep"
)

func TestElasticsearchServiceToBrokerAPIService(t *testing.T) {
	redisService := ElasticsearchService{
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
		ElasticsearchPlans: []ElasticsearchPlan{
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

func TestElasticsearchPlanCanUpgradeTo(t *testing.T) {
	// helper plans
	mediumNonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-medium-memory-optimized"}, InstanceType: "r8g.medium.search", DataCount: "2"}
	largeNonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-large-memory-optimized"}, InstanceType: "r8g.large.search", DataCount: "2"}
	xlargeNonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-xlarge-memory-optimized"}, InstanceType: "r8g.xlarge.search", DataCount: "2"}
	singleNode := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-dev"}, InstanceType: "r8g.large.search", DataCount: "1"}
	mediumHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-medium-memory-optimized-ha"}, InstanceType: "r8g.medium.search", DataCount: "4"}
	largeHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-large-memory-optimized-ha"}, InstanceType: "r8g.large.search", DataCount: "4"}
	unknown := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-mystery"}, InstanceType: "z9z.mystery.search", DataCount: "2"}
	esDev := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-dev"}, InstanceType: "t3.small.search", DataCount: "1"}
	esDevMigration := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-dev-6.8-migration"}, InstanceType: "t3.small.search", DataCount: "1"}

	testCases := map[string]struct {
		from      ElasticsearchPlan
		to        ElasticsearchPlan
		expectOK  bool
		expectMsg string
	}{
		"non-HA same size allowed": {
			from:     largeNonHA,
			to:       largeNonHA,
			expectOK: true,
		},
		"non-HA upgrade to larger allowed": {
			from:     mediumNonHA,
			to:       largeNonHA,
			expectOK: true,
		},
		"non-HA upgrade two steps allowed": {
			from:     mediumNonHA,
			to:       xlargeNonHA,
			expectOK: true,
		},
		"non-HA downgrade blocked": {
			from:      largeNonHA,
			to:        mediumNonHA,
			expectOK:  false,
			expectMsg: "downgrading",
		},
		"HA upgrade to larger allowed": {
			from:     mediumHA,
			to:       largeHA,
			expectOK: true,
		},
		"HA same size allowed": {
			from:     largeHA,
			to:       largeHA,
			expectOK: true,
		},
		"HA downgrade blocked": {
			from:      largeHA,
			to:        mediumHA,
			expectOK:  false,
			expectMsg: "downgrading",
		},
		"non-HA to HA blocked": {
			from:      mediumNonHA,
			to:        mediumHA,
			expectOK:  false,
			expectMsg: "highly-available",
		},
		"HA to non-HA blocked": {
			from:      largeHA,
			to:        largeNonHA,
			expectOK:  false,
			expectMsg: "highly-available",
		},
		"single node to HA blocked": {
			from:      singleNode,
			to:        mediumHA,
			expectOK:  false,
			expectMsg: "highly-available",
		},
		"unknown target instance type blocked": {
			from:      mediumNonHA,
			to:        unknown,
			expectOK:  false,
			expectMsg: "unable to determine plan sizes",
		},
		"single node to multi-node non-HA blocked": {
			from:      esDev,
			to:        mediumNonHA,
			expectOK:  false,
			expectMsg: "single-node and multi-node",
		},
		"single node to same single node allowed": {
			from:     esDev,
			to:       esDev,
			expectOK: true,
		},
		"single node to other single node plan allowed": {
			from:     esDevMigration,
			to:       esDev,
			expectOK: true,
		},
		"multi-node to single node blocked": {
			from:      mediumNonHA,
			to:        esDev,
			expectOK:  false,
			expectMsg: "single-node and multi-node",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ok, msg := tc.from.CanUpgradeTo(tc.to)
			if ok != tc.expectOK {
				t.Fatalf("expected ok=%v, got %v (msg=%q)", tc.expectOK, ok, msg)
			}
			if !tc.expectOK && tc.expectMsg != "" && !contains(msg, tc.expectMsg) {
				t.Fatalf("expected message containing %q, got %q", tc.expectMsg, msg)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
