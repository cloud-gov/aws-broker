package elasticsearch

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestValidateVolumeType(t *testing.T) {
	testCases := map[string]struct {
		storageType string
		expectedErr bool
	}{
		"invalid": {
			storageType: "io1",
			expectedErr: true,
		},
		"empty": {
			storageType: "",
			expectedErr: false,
		},
		"gp2": {
			storageType: "gp2",
			expectedErr: true,
		},
		"gp3": {
			storageType: "gp3",
			expectedErr: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateVolumeType(aws.String(test.storageType))
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}
