package elasticsearch

import (
	"testing"

	"github.com/cloud-gov/aws-broker/config"
)

func TestValidate(t *testing.T) {
	testCases := map[string]struct {
		options     ElasticsearchOptions
		settings    *config.Settings
		expectedErr bool
	}{
		"accepted volume type": {
			options: ElasticsearchOptions{
				VolumeType: "gp3",
			},
			settings:    &config.Settings{},
			expectedErr: false,
		},
		"invalid volume type": {
			options: ElasticsearchOptions{
				VolumeType: "io1",
			},
			settings:    &config.Settings{},
			expectedErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.options.Validate(test.settings)
			if test.expectedErr && err == nil {
				t.Fatalf("expected error")
			}
			if !test.expectedErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}
