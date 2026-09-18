package catalog

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
			ok, err := tc.from.CanUpgradeTo(tc.to)
			if ok != tc.expectOK {
				t.Fatalf("expected ok=%v, got %v (err=%v)", tc.expectOK, ok, err)
			}
			if tc.expectOK {
				if err != nil {
					t.Fatalf("expected no error for an allowed plan change, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected an error explaining the rejection, got nil")
			}
			if tc.expectMsg != "" && !strings.Contains(err.Error(), tc.expectMsg) {
				t.Fatalf("expected message containing %q, got %q", tc.expectMsg, err.Error())
			}
		})
	}
}

func TestElasticsearchCatalogPlanSizeRanks(t *testing.T) {
	catalog := parseCatalogTemplate(t)
	plans := catalog.ElasticsearchService.ElasticsearchPlans

	if len(plans) == 0 {
		t.Fatal("parsed no Elasticsearch plans from catalog-template.yml")
	}

	byName := make(map[string]ElasticsearchPlan, len(plans))
	for _, plan := range plans {
		byName[plan.Name] = plan
	}

	for _, plan := range plans {
		t.Run(plan.Name, func(t *testing.T) {
			if plan.InstanceSizeRank <= 0 {
				t.Errorf("plan has instanceSizeRank=%d; every plan needs a positive rank or CanUpgradeTo cannot compare it",
					plan.InstanceSizeRank)
			}
			if plan.SizeRank() < 0 {
				t.Errorf("SizeRank()=%d; plan cannot participate in any plan change", plan.SizeRank())
			}
			if plan.dataCount() <= 0 {
				t.Errorf("dataCount=%q does not parse to a positive node count", plan.DataCount)
			}
		})
	}

	// Every -ha plan must share its tier's rank and add data nodes. This is what
	// makes the intended non-HA -> same-tier-HA upgrade reachable: SizeRank breaks
	// the tie on data-node count only when the instance rank is equal.
	for _, plan := range plans {
		base, isHA := strings.CutSuffix(plan.Name, "-ha")
		if !isHA {
			continue
		}
		t.Run(plan.Name+" pairs with "+base, func(t *testing.T) {
			counterpart, found := byName[base]
			if !found {
				t.Fatalf("no non-HA counterpart %q found for HA plan", base)
			}
			if counterpart.InstanceSizeRank != plan.InstanceSizeRank {
				t.Errorf("HA plan rank %d != non-HA counterpart rank %d; the pair is not the same tier",
					plan.InstanceSizeRank, counterpart.InstanceSizeRank)
			}
			if plan.dataCount() <= counterpart.dataCount() {
				t.Errorf("HA plan has %d data nodes, counterpart has %d; HA must add nodes",
					plan.dataCount(), counterpart.dataCount())
			}
			if ok, err := counterpart.CanUpgradeTo(plan); !ok {
				t.Errorf("%s -> %s must be an allowed upgrade, got: %v", base, plan.Name, err)
			}
		})
	}
}

// TestParseCatalogRejectsMissingSizeRank pins the fail-closed behaviour: an
// Elasticsearch plan with no instanceSizeRank must be rejected at catalog load,
// not accepted and then found unrankable at the first upgrade attempt.
func TestParseCatalogRejectsMissingSizeRank(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "catalog-test.yml"))
	if err != nil {
		t.Fatal(err)
	}

	// Sanity check: the fixture validates as-is, so a failure below is caused by
	// the removal and not by unrelated drift in catalog-test.yml.
	if _, err := parseCatalog(data); err != nil {
		t.Fatalf("catalog-test.yml does not validate before mutation: %v", err)
	}

	stripped := regexp.MustCompile(`(?m)^\s*instanceSizeRank:.*\n`).ReplaceAll(data, nil)
	if bytes.Equal(stripped, data) {
		t.Fatal("catalog-test.yml contains no instanceSizeRank lines to remove; test cannot prove anything")
	}

	if _, err := parseCatalog(stripped); err == nil {
		t.Fatal("parseCatalog accepted a catalog whose Elasticsearch plans have no instanceSizeRank")
	} else if !strings.Contains(err.Error(), "InstanceSizeRank") {
		t.Errorf("expected the validation error to name InstanceSizeRank, got: %v", err)
	}
}

func parseCatalogTemplate(t *testing.T) *Catalog {
	t.Helper()

	// catalog-template.yml is the source of truth that spruce renders into the
	// deployed catalog.yml, so it is what must be asserted against; the rendered
	// catalog.yml is gitignored and absent in a clean checkout.
	data, err := os.ReadFile(filepath.Join("..", "catalog-template.yml"))
	if err != nil {
		t.Fatalf("reading catalog-template.yml: %v", err)
	}

	catalog, err := parseCatalog(data)
	if err != nil {
		t.Fatalf("parsing catalog-template.yml: %v", err)
	}
	return catalog
}
