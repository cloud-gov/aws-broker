package tags

import (
	"fmt"

	brokertags "github.com/cloud-gov/go-broker-tags"
)

func GenerateTags(tagManager brokertags.TagManager, serviceOfferingName string, planName string, resourceGUIDs brokertags.ResourceGUIDs) (map[string]string, error) {
	generatedTags, err := tagManager.GenerateTags(
		brokertags.Update,
		serviceOfferingName,
		planName,
		resourceGUIDs,
		true,
	)
	if err != nil {
		return map[string]string{}, fmt.Errorf("error generating new tags for database %s", err)
	}
	// We can ignore the timestamp tags, if they exist
	delete(generatedTags, "Created at")
	delete(generatedTags, "Updated at")
	return generatedTags, nil
}
