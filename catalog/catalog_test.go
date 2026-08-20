package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

var rdsPGTestPlanID = "da91e15c-98c9-46a9-b114-02b8d28062c6"
var rdsPGValidVersion = "15"
var rdsPGInvalidVersion = "9.6"

var rdsMySQLTestPlanID = "da91e15c-98c9-46a9-b114-02b8d28062c7"
var rdsMySQLValidVersion = "8.4"
var rdsMySQLInvalidVersion = "5.6"

var rdsOracleSE2TestPlanID = "da91e15c-98c9-46a9-b114-02b8d28062c8"
var rdsOracleSE2RedundantTestPlanID = "da91e15c-98c9-46a9-b114-02b8d28062c9"

// Helper function to call os.Getwd with error checking
func checkedGetwd(t *testing.T) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error calling os.Getwd(): %s", err)
	}
	return wd
}

func TestInitCatalog(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)
	if catalog == nil {
		t.Error("Did not read catalog")
	}
}

func TestCatalogGetServices(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)
	if catalog == nil {
		t.Fatal("Did not read catalog")
	}
	services := catalog.GetServices()
	if len(services) != 3 {
		t.Fatalf("expected 3 services, got %d", len(services))
	}
}

func TestFetchPlan(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	_, err := catalog.RdsService.FetchPlan(rdsPGTestPlanID)

	if err != nil {
		t.Error("Could not fetch plan " + rdsPGTestPlanID)
	}
}

func TestRDSCheckPlanUpdatable(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	plan, err := catalog.RdsService.FetchPlan(rdsPGTestPlanID)

	if err != nil {
		t.Error("Could not fetch plan " + rdsPGTestPlanID)
	}

	if plan.PlanUpdatable == nil {
		t.Error("PlanUpdatable property not set")
	}
}

func TestRDSPGCheckVersion(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	plan, err := catalog.RdsService.FetchPlan(rdsPGTestPlanID)

	if err != nil {
		t.Error("Could not fetch plan " + rdsPGTestPlanID)
	}

	// Test that a valid version returns true.
	validVersion := plan.CheckVersion(rdsPGValidVersion)

	if !validVersion {
		t.Error("Valid RDS version check failed.")
	}

	// Test that an invalid version returns false.
	validVersion = plan.CheckVersion(rdsPGInvalidVersion)

	if validVersion {
		t.Error("Invalid RDS version check failed.")
	}
}

func TestRDSMySQLCheckVersion(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	plan, err := catalog.RdsService.FetchPlan(rdsMySQLTestPlanID)

	if err != nil {
		t.Error("Could not fetch plan " + rdsMySQLTestPlanID)
	}

	// Test that a valid version returns true.
	validVersion := plan.CheckVersion(rdsMySQLValidVersion)

	if !validVersion {
		t.Error("Valid RDS version check failed.")
	}

	// Test that an invalid version returns false.
	validVersion = plan.CheckVersion(rdsMySQLInvalidVersion)

	if validVersion {
		t.Error("Invalid RDS version check failed.")
	}
}

func TestRDSOracleSE2Plans(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	// Non-redundant (Single-AZ) Oracle SE2 plan.
	base, err := catalog.RdsService.FetchPlan(rdsOracleSE2TestPlanID)
	if err != nil {
		t.Fatal("Could not fetch plan " + rdsOracleSE2TestPlanID)
	}
	if base.DbType != "oracle-se2" {
		t.Errorf("base Oracle plan DbType = %q, want oracle-se2", base.DbType)
	}
	if base.Redundant {
		t.Error("base medium-oracle-se2 plan must be Single-AZ (Redundant=false)")
	}

	// Redundant (Multi-AZ) Oracle SE2 plan.
	redundant, err := catalog.RdsService.FetchPlan(rdsOracleSE2RedundantTestPlanID)
	if err != nil {
		t.Fatal("Could not fetch plan " + rdsOracleSE2RedundantTestPlanID)
	}
	if redundant.DbType != "oracle-se2" {
		t.Errorf("redundant Oracle plan DbType = %q, want oracle-se2", redundant.DbType)
	}
	if !redundant.Redundant {
		t.Error("medium-oracle-se2-redundant plan must be Multi-AZ (Redundant=true)")
	}
	// Oracle SE2 is not ReadReplicaCapable on RDS; the redundant plan must not
	// request a read replica (would otherwise trip the ReadReplica && Redundant path).
	if redundant.ReadReplica {
		t.Error("Oracle SE2 redundant plan must not enable read_replica (SE2 is not replica-capable)")
	}
}

func TestRDSCheckVersionEmpty(t *testing.T) {
	wd := checkedGetwd(t)
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	plan, err := catalog.RdsService.FetchPlan(rdsPGTestPlanID)

	if err != nil {
		t.Error("Could not fetch plan " + rdsPGTestPlanID)
	}

	// Test that no versions set in the plan returns true if a version is
	// specified.
	validVersion := plan.CheckVersion(rdsPGValidVersion)

	if !validVersion {
		t.Error("Empty RDS version check failed.")
	}
}
