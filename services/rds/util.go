package rds

import "fmt"

func validateBinaryLogFormat(format string) error {
	switch format {
	case "ROW", "STATEMENT", "MIXED":
		return nil
	default:
		return fmt.Errorf("invalid binary log format %s", format)
	}
}
