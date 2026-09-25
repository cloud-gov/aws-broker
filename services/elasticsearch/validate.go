package elasticsearch

import "fmt"

func validateVolumeType(volumeType *string) error {
	if volumeType == nil {
		return nil
	}
	switch *volumeType {
	case "", "gp3":
		return nil
	default:
		return fmt.Errorf("volume type is not supported: %s", *volumeType)
	}
}
