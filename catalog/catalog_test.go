package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

var rdsPGTestPlanID = "da91e15c-98c9-46a9-b114-02b8d28062c6"
var rdsPGValidVersion = "10"
var rdsPGInvalidVersion = "9.6"

var rdsMySQLTestPlanID = "da91e15c-98c9-46a9-b114-02b8d28062c7"
var rdsMySQLValidVersion = "8.0"
var rdsMySQLInvalidVersion = "5.6"

var rdsOracleTestPlanID = "332e0168-6969-4bd7-b07f-29f08c4bf78f"

func TestInitCatalog(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)
	if catalog == nil {
		t.Error("Did not read catalog")
	}
}

func TestFetchPlan(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "..")
	catalog := InitCatalog(path)

	_, err := catalog.RdsService.FetchPlan(rdsPGTestPlanID)

	if err != nil {
		t.Error("Could not fetch plan " + rdsPGTestPlanID)
	}
}

func TestRDSPGCheckVersion(t *testing.T) {
	wd, _ := os.Getwd()
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
	wd, _ := os.Getwd()
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

func TestRDSCheckVersionEmpty(t *testing.T) {
	wd, _ := os.Getwd()
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
