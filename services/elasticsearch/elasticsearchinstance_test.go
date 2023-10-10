package elasticsearch

import (
	"testing"

	"github.com/go-test/deep"
)

func TestUpdateInstance(t *testing.T) {
	testCases := map[string]struct {
		options          ElasticsearchOptions
		existingInstance *ElasticsearchInstance
		expectedInstance *ElasticsearchInstance
		expectErr        bool
	}{
		"gp3 upgrade succeeds": {
			options: ElasticsearchOptions{
				VolumeType: "gp3",
			},
			existingInstance: &ElasticsearchInstance{
				VolumeType: "gp2",
			},
			expectedInstance: &ElasticsearchInstance{
				VolumeType: "gp3",
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.existingInstance.update(test.options)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if test.expectErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if diff := deep.Equal(test.existingInstance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}
}
