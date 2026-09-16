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

	mediumNonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-medium-memory-optimized"}, InstanceType: "r8g.medium.search", InstanceSizeRank: 20, DataCount: "2"}
	largeNonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-large-memory-optimized"}, InstanceType: "r8g.large.search", InstanceSizeRank: 30, DataCount: "2"}
	xlargeNonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-xlarge-memory-optimized"}, InstanceType: "r8g.xlarge.search", InstanceSizeRank: 40, DataCount: "2"}
	singleNode := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-dev"}, InstanceType: "r8g.large.search", InstanceSizeRank: 30, DataCount: "1"}
	mediumHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-medium-memory-optimized-ha"}, InstanceType: "r8g.medium.search", InstanceSizeRank: 20, DataCount: "4"}
	largeHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-large-memory-optimized-ha"}, InstanceType: "r8g.large.search", InstanceSizeRank: 30, DataCount: "4"}
	unranked := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-mystery"}, InstanceType: "z9z.mystery.search", DataCount: "2"}
	esDev := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-dev"}, InstanceType: "t3.small.search", InstanceSizeRank: 10, DataCount: "1"}
	esDevMigration := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-dev-6.8-migration"}, InstanceType: "t3.small.search", InstanceSizeRank: 10, DataCount: "1"}
	mediumC5NonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-medium"}, InstanceType: "c5.large.search", InstanceSizeRank: 20, DataCount: "2"}
	largeC5NonHA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-large"}, InstanceType: "c5.xlarge.search", InstanceSizeRank: 30, DataCount: "2"}
	mediumC5HA := ElasticsearchPlan{ServicePlan: domain.ServicePlan{Name: "es-medium-ha"}, InstanceType: "c5.large.search", InstanceSizeRank: 20, DataCount: "4"}

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
		"non-HA to HA allowed": {
			from:     mediumNonHA,
			to:       mediumHA,
			expectOK: true,
		},
		"non-HA to larger tier HA allowed": {
			from:     mediumNonHA,
			to:       largeHA,
			expectOK: true,
		},
		"non-HA to smaller tier HA blocked": {
			from:      largeNonHA,
			to:        mediumHA,
			expectOK:  false,
			expectMsg: "downgrading",
		},
		"HA to non-HA blocked": {
			from:      largeHA,
			to:        largeNonHA,
			expectOK:  false,
			expectMsg: "reduce the number of data nodes",
		},
		"HA to larger tier non-HA blocked": {
			from:      mediumHA,
			to:        largeNonHA,
			expectOK:  false,
			expectMsg: "reduce the number of data nodes",
		},
		"single node to HA blocked": {
			from:      singleNode,
			to:        mediumHA,
			expectOK:  false,
			expectMsg: "single-node plan to a multi-node plan",
		},
		"unranked target blocked": {
			from:      mediumNonHA,
			to:        unranked,
			expectOK:  false,
			expectMsg: "unable to determine plan sizes",
		},
		"unranked source blocked": {
			from:      unranked,
			to:        largeNonHA,
			expectOK:  false,
			expectMsg: "unable to determine plan sizes",
		},
		"single node to multi-node non-HA blocked": {
			from:      esDev,
			to:        mediumNonHA,
			expectOK:  false,
			expectMsg: "single-node plan to a multi-node plan",
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
			expectMsg: "reduce the number of data nodes",
		},
		"same tier c5 to memory-optimized allowed": {
			from:     mediumC5NonHA,
			to:       mediumNonHA,
			expectOK: true,
		},
		"same tier memory-optimized to c5 allowed": {
			from:     mediumNonHA,
			to:       mediumC5NonHA,
			expectOK: true,
		},
		"same tier family switch allowed at large tier": {
			from:     largeC5NonHA,
			to:       largeNonHA,
			expectOK: true,
		},
		"cross-family upgrade to larger tier allowed": {
			from:     mediumC5NonHA,
			to:       largeNonHA,
			expectOK: true,
		},
		"cross-family downgrade to smaller tier blocked": {
			from:      largeC5NonHA,
			to:        mediumNonHA,
			expectOK:  false,
			expectMsg: "downgrading",
		},
		"cross-family non-HA to same tier HA allowed": {
			from:     mediumC5NonHA,
			to:       mediumHA,
			expectOK: true,
		},
		"cross-family same tier HA to HA allowed": {
			from:     mediumC5HA,
			to:       mediumHA,
			expectOK: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ok, msg := tc.from.CanUpgradeTo(tc.to)
			if ok != tc.expectOK {
				t.Fatalf("expected ok=%v, got %v (msg=%q)", tc.expectOK, ok, msg)
			}
			if !tc.expectOK && tc.expectMsg != "" && !strings.Contains(msg, tc.expectMsg) {
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
