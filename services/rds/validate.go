package rds

import (
	"fmt"
	"slices"
)

func validateBinaryLogFormat(format string) error {
	switch format {
	case "", "ROW", "STATEMENT", "MIXED":
		return nil
	default:
		return fmt.Errorf("invalid binary log format: %s", format)
	}
}

func validateStorageType(storageType string) error {
	switch storageType {
	case "", "gp3":
		return nil
	default:
		return fmt.Errorf("storage type is not supported: %s", storageType)
	}
}

func validateLongQueryTime(v *float64) error {
	if v == nil {
		return nil
	}
	if *v < 0 {
		return fmt.Errorf("long_query_time must be >= 0, got %v", *v)
	}

	return nil
}

var validLogStatementValues = []string{"none", "ddl", "mod", "all"}
var validLogConnectionsBoolValues = []string{"true", "false"}
var validLogConnectionsStringValues = []string{"receipt", "authentication", "authorization", "setup_durations", "all"}

func validatePgQueryLogging(opts *PgQueryLoggingOptions) error {
	if opts == nil {
		return nil
	}
	if opts.LogMinDurationStatement != nil && *opts.LogMinDurationStatement < -1 {
		return fmt.Errorf("log_min_duration_statement must be >= -1, got %d", *opts.LogMinDurationStatement)
	}
	if opts.LogMinDurationSample != nil && *opts.LogMinDurationSample < -1 {
		return fmt.Errorf("log_min_duration_sample must be >= -1, got %d", *opts.LogMinDurationSample)
	}
	if opts.LogStatement != nil {
		valid := slices.Contains(validLogStatementValues, *opts.LogStatement)
		if !valid {
			return fmt.Errorf("log_statement must be one of %v, got %q", validLogStatementValues, *opts.LogStatement)
		}
	}
	if opts.LogStatementSampleRate != nil && (*opts.LogStatementSampleRate < 0.0 || *opts.LogStatementSampleRate > 1.0) {
		return fmt.Errorf("log_statement_sample_rate must be between 0.0 and 1.0, got %v", *opts.LogStatementSampleRate)
	}
	if opts.LogConnections != nil {
		allValidLogConnectionsValues := append(validLogConnectionsBoolValues, validLogConnectionsStringValues...)
		valid := slices.Contains(allValidLogConnectionsValues, *opts.LogConnections)
		if !valid {
			return fmt.Errorf("log_connections must be one of %v, got %q", allValidLogConnectionsValues, *opts.LogConnections)
		}
	}

	return nil
}

// validateOracleOptions enforces an ALLOWLIST for Oracle create/update parameters.
// Oracle is a self-service, born-hardened offering; a customer must not be
// able to (a) use MySQL/Postgres-only knobs that don't apply, (b) weaken the STIG
// baseline, or (c) turn on public accessibility. Unknown-but-harmless params are
// tolerated; the ones below are the meaningful footguns. This is called only for
// Oracle engines, so postgres/mysql behavior is unchanged.
func validateOracleOptions(o Options) error {
	// Attack-surface / STIG-baseline protection: publicly_accessible must never be
	// set true for Oracle (plan is private-only).
	if o.PubliclyAccessible {
		return fmt.Errorf("publicly_accessible is not permitted for Oracle instances (private-only)")
	}
	// MySQL-only knobs are meaningless on Oracle — reject to avoid silent no-ops
	// that mislead a self-service customer.
	if o.EnableFunctions {
		return fmt.Errorf("enable_functions is a MySQL-only option and is not supported for Oracle")
	}
	if o.BinaryLogFormat != "" {
		return fmt.Errorf("binary_log_format is a MySQL-only option and is not supported for Oracle")
	}
	// PostgreSQL-only knobs.
	if o.EnablePgCron != nil {
		return fmt.Errorf("enable_pg_cron is a PostgreSQL-only option and is not supported for Oracle")
	}
	if o.PgQueryLogging != nil {
		return fmt.Errorf("pg_query_logging is a PostgreSQL-only option and is not supported for Oracle")
	}
	// In-place version change is not supported for Oracle yet.
	if o.Version != "" {
		return fmt.Errorf("version selection is not yet supported for Oracle; the plan pins the engine version")
	}
	// Major-version upgrade is likewise not supported for Oracle (the baseline
	// pins the version); reject rather than accept a flag that is a silent no-op.
	if o.AllowMajorVersionUpgrade != nil {
		return fmt.Errorf("allow_major_version_upgrade is not supported for Oracle; the plan pins the engine version")
	}
	// Log exports must be within the Oracle-supported allowlist. The allowlist is
	// derived from the embedded baseline (log_exports.yml `supported`) so the two
	// cannot drift; fail closed if the baseline cannot be loaded.
	valid, err := validOracleLogExports()
	if err != nil {
		return fmt.Errorf("validateOracleOptions: %w", err)
	}
	for _, e := range o.EnableCloudWatchLogGroupExports {
		if !slices.Contains(valid, e) {
			return fmt.Errorf("unsupported Oracle CloudWatch log export %q; valid options: %v", e, valid)
		}
	}
	return nil
}

// validOracleLogExports returns the allowlist of CloudWatch log-export types a
// customer may request for an Oracle instance. It is sourced from the embedded
// baseline's `supported` set (baselines/oracle19c/log_exports.yml) rather than a
// hand-maintained literal, so the customer allowlist and the baseline stay in sync.
func validOracleLogExports() ([]string, error) {
	f, err := loadOracleLogExports()
	if err != nil {
		return nil, err
	}
	return f.Supported, nil
}
