package rds

import (
	"embed"
	"fmt"

	"gopkg.in/yaml.v3"
)

// baselines.go — loads the structured, embedded Oracle 19c baseline data
// (epic #519, WS5/6/7). Keeping the hardened values in reviewable YAML (rather
// than Go literals) makes the STIG posture auditable and reusable by a future CSB
// brokerpak (ADR-0003). The files are embedded so the broker binary is
// self-contained.

//go:embed baselines/oracle19c/*.yml
var oracle19cBaselineFS embed.FS

// oracleParameter is one RDS parameter-group entry in the Oracle baseline.
type oracleParameter struct {
	Name        string `yaml:"name"`
	Value       string `yaml:"value"`
	ApplyMethod string `yaml:"apply_method"`
	StigIntent  string `yaml:"stig_intent"`
}

type oracleParametersFile struct {
	EngineFamilyPrefix string            `yaml:"engine_family_prefix"`
	Parameters         []oracleParameter `yaml:"parameters"`
}

// oracleLogExportsFile lists the CloudWatch log-export types enabled by default.
type oracleLogExportsFile struct {
	DefaultExports []string `yaml:"default_exports"`
	Supported      []string `yaml:"supported"`
}

// oracleOptionsFile lists broker-managed option-group options.
type oracleOptionsFile struct {
	Options []oracleOption `yaml:"options"`
}

type oracleOption struct {
	Name       string `yaml:"name"`
	StigIntent string `yaml:"stig_intent"`
	Notes      string `yaml:"notes"`
}

// loadOracleParameters parses baselines/oracle19c/parameters.yml.
func loadOracleParameters() (*oracleParametersFile, error) {
	b, err := oracle19cBaselineFS.ReadFile("baselines/oracle19c/parameters.yml")
	if err != nil {
		return nil, fmt.Errorf("oracle baseline: read parameters.yml: %w", err)
	}
	var f oracleParametersFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("oracle baseline: parse parameters.yml: %w", err)
	}
	return &f, nil
}

// loadOracleLogExports parses baselines/oracle19c/log_exports.yml.
func loadOracleLogExports() (*oracleLogExportsFile, error) {
	b, err := oracle19cBaselineFS.ReadFile("baselines/oracle19c/log_exports.yml")
	if err != nil {
		return nil, fmt.Errorf("oracle baseline: read log_exports.yml: %w", err)
	}
	var f oracleLogExportsFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("oracle baseline: parse log_exports.yml: %w", err)
	}
	return &f, nil
}

// loadOracleOptions parses baselines/oracle19c/options.yml.
func loadOracleOptions() (*oracleOptionsFile, error) {
	b, err := oracle19cBaselineFS.ReadFile("baselines/oracle19c/options.yml")
	if err != nil {
		return nil, fmt.Errorf("oracle baseline: read options.yml: %w", err)
	}
	var f oracleOptionsFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("oracle baseline: parse options.yml: %w", err)
	}
	return &f, nil
}
