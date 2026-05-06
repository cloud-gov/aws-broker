package rds

import "fmt"

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
