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
var validLogConnectionsValues = []string{"true", "false", "receipt", "authentication", "authorization", "setup_durations", "all"}

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
		valid := slices.Contains(validLogConnectionsValues, *opts.LogConnections)
		if !valid {
			return fmt.Errorf("log_connections must be one of %v, got %q", validLogConnectionsValues, *opts.LogConnections)
		}
	}

	return nil
}
