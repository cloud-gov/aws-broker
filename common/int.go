package common

import (
	"fmt"
	"math"
)

func ConvertIntToInt32Safely(value int) (*int32, error) {
	if value < 0 || value > math.MaxInt32 {
		return nil, fmt.Errorf("invalid value %q, must be between 0 and %d", value, math.MaxInt32)
	}
	int32Value := int32(value)
	return &int32Value, nil
}

func ConvertInt64ToInt32Safely(value int64) (*int32, error) {
	if value < 0 || value > math.MaxInt32 {
		return nil, fmt.Errorf("invalid value %q, must be between 0 and %d", value, math.MaxInt32)
	}
	int32Value := int32(value)
	return &int32Value, nil
}
