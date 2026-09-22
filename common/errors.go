package common

import "fmt"

func FmtErr(outerErr error, innerErr error) error {
	return fmt.Errorf("%s: %w", outerErr, innerErr)
}
