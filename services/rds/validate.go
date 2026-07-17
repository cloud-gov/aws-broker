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

// validOracleLogExports is the allowlist of CloudWatch log-export types a
// customer may request for an Oracle instance (RDS Oracle supported set).
var validOracleLogExports = []string{"alert", "audit", "listener", "trace", "oemagent"}

// validateOracleOptions enforces an ALLOWLIST for Oracle create/update parameters
// (#535). Oracle is a self-service, born-hardened offering; a customer must not be
// able to (a) use MySQL/Postgres-only knobs that don't apply, (b) weaken the STIG
// baseline, or (c) turn on public accessibility. Unknown-but-harmless params are
// tolerated; the ones below are the meaningful footguns. This is called only for
// Oracle engines, so postgres/mysql behavior is unchanged.
func validateOracleOptions(o Options) error {
	// Attack-surface / STIG-baseline protection: publicly_accessible must never be
	// set true for Oracle (plan is private-only, ADR-0004).
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
	// In-place version change is not supported for Oracle yet (#524 follow-up).
	if o.Version != "" {
		return fmt.Errorf("version selection is not yet supported for Oracle; the plan pins the engine version")
	}
	// Log exports must be within the Oracle-supported allowlist.
	for _, e := range o.EnableCloudWatchLogGroupExports {
		if !slices.Contains(validOracleLogExports, e) {
			return fmt.Errorf("unsupported Oracle CloudWatch log export %q; valid options: %v", e, validOracleLogExports)
		}
	}
	return nil
}
