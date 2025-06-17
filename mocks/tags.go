package mocks

import (
	brokertags "github.com/cloud-gov/go-broker-tags"
)

type MockTagGenerator struct {
	tags map[string]string
}

func (mt *MockTagGenerator) GenerateTags(
	action brokertags.Action,
	serviceName string,
	servicePlanName string,
	resourceGUIDs brokertags.ResourceGUIDs,
	getMissingResources bool,
) (map[string]string, error) {
	return mt.tags, nil
}
