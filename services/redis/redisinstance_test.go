package redis

import (
	"testing"

	"code.cloudfoundry.org/brokerapi/v13/domain"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	"github.com/go-test/deep"
)

func TestInstanceInit(t *testing.T) {
	testCases := map[string]struct {
		planID           string
		uuid             string
		orgID            string
		spaceID          string
		serviceID        string
		plan             catalog.RedisPlan
		catalog          *catalog.Catalog
		redisBroker      *redisBroker
		settings         config.Settings
		tags             map[string]string
		options          RedisOptions
		expectedInstance *RedisInstance
	}{
		"success": {
			uuid:      "uuid-1",
			serviceID: "service-1",
			orgID:     "org-1",
			spaceID:   "space-1",
			plan: catalog.RedisPlan{
				Tags: map[string]string{
					"plan-tag-1": "foo",
				},
				ServicePlan: domain.ServicePlan{
					ID:          "plan-1",
					Description: "test description",
				},
				SubnetGroup:                "subnet-1",
				SecurityGroup:              "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
			},
			tags: map[string]string{
				"tag-1": "bar",
			},
			settings: config.Settings{
				EncryptionKey:     helpers.RandStr(16),
				DbShorthandPrefix: "prefix",
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
						ServiceID:        "service-1",
						PlanID:           "plan-1",
					},
				},
				Description:                "test description",
				DbSubnetGroup:              "subnet-1",
				SecGroup:                   "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
				ClusterID:                  "prefix-uuid-1",
				Tags: map[string]string{
					"plan-tag-1": "foo",
					"tag-1":      "bar",
				},
			},
		},
		"sets engine version from plan": {
			uuid:      "uuid-1",
			serviceID: "service-1",
			orgID:     "org-1",
			spaceID:   "space-1",
			plan: catalog.RedisPlan{
				Tags: map[string]string{
					"plan-tag-1": "foo",
				},
				ServicePlan: domain.ServicePlan{
					ID:          "plan-1",
					Description: "test description",
				},
				SubnetGroup:                "subnet-1",
				SecurityGroup:              "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
				EngineVersion:              "version-1",
			},
			tags: map[string]string{
				"tag-1": "bar",
			},
			settings: config.Settings{
				EncryptionKey:     helpers.RandStr(16),
				DbShorthandPrefix: "prefix",
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
						ServiceID:        "service-1",
						PlanID:           "plan-1",
					},
				},
				Description:                "test description",
				DbSubnetGroup:              "subnet-1",
				SecGroup:                   "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
				ClusterID:                  "prefix-uuid-1",
				EngineVersion:              "version-1",
				Tags: map[string]string{
					"plan-tag-1": "foo",
					"tag-1":      "bar",
				},
			},
		},
		"sets engine version from options": {
			uuid:      "uuid-1",
			serviceID: "service-1",
			orgID:     "org-1",
			spaceID:   "space-1",
			plan: catalog.RedisPlan{
				Tags: map[string]string{
					"plan-tag-1": "foo",
				},
				ServicePlan: domain.ServicePlan{
					ID:          "plan-1",
					Description: "test description",
				},
				SubnetGroup:                "subnet-1",
				SecurityGroup:              "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
				EngineVersion:              "version-1",
			},
			tags: map[string]string{
				"tag-1": "bar",
			},
			options: RedisOptions{
				EngineVersion: "version-2",
			},
			settings: config.Settings{
				EncryptionKey:     helpers.RandStr(16),
				DbShorthandPrefix: "prefix",
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
					Uuid: "uuid-1",
					Request: request.Request{
						OrganizationGUID: "org-1",
						SpaceGUID:        "space-1",
						ServiceID:        "service-1",
						PlanID:           "plan-1",
					},
				},
				Description:                "test description",
				DbSubnetGroup:              "subnet-1",
				SecGroup:                   "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
				ClusterID:                  "prefix-uuid-1",
				EngineVersion:              "version-2",
				Tags: map[string]string{
					"plan-tag-1": "foo",
					"tag-1":      "bar",
				},
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			instance := &RedisInstance{}
			instance.init(
				test.uuid,
				test.orgID,
				test.spaceID,
				test.serviceID,
				test.plan,
				test.options,
				&test.settings,
				test.tags,
			)
			if diff := deep.Equal(instance, test.expectedInstance); diff != nil {
				t.Error(diff)
			}
		})
	}

}

func TestInstanceModify(t *testing.T) {
	testCases := map[string]struct {
		existingInstance *RedisInstance
		newPlan          catalog.RedisPlan
		catalog          *catalog.Catalog
		redisBroker      *redisBroker
		tags             map[string]string
		options          RedisOptions
		expectedInstance *RedisInstance
		expectUpdates    bool
	}{
		"sets plan properties": {
			existingInstance: &RedisInstance{},
			newPlan: catalog.RedisPlan{
				Tags: map[string]string{
					"plan-tag-1": "foo",
				},
				ServicePlan: domain.ServicePlan{
					ID:          "plan-1",
					Description: "test description",
				},
				SubnetGroup:                "subnet-1",
				SecurityGroup:              "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
			},
			expectedInstance: &RedisInstance{
				Instance: base.Instance{
					Request: request.Request{
						PlanID: "plan-1",
					},
				},
				Description:                "test description",
				DbSubnetGroup:              "subnet-1",
				SecGroup:                   "sec-group-1",
				NumCacheClusters:           1,
				CacheNodeType:              "type-1",
				PreferredMaintenanceWindow: "12 AM",
				SnapshotWindow:             "3 AM",
				SnapshotRetentionLimit:     14,
				AutomaticFailoverEnabled:   true,
				Tags: map[string]string{
					"plan-tag-1": "foo",
				},
			},
			expectUpdates: true,
		},
		"sets engine version from plan": {
			existingInstance: &RedisInstance{
				EngineVersion: "version-1",
			},
			newPlan: catalog.RedisPlan{
				EngineVersion: "version-2",
			},
			expectedInstance: &RedisInstance{
				EngineVersion: "version-2",
			},
			expectUpdates: true,
		},
		"sets engine version from options": {
			existingInstance: &RedisInstance{
				EngineVersion: "version-1",
			},
			newPlan: catalog.RedisPlan{
				EngineVersion: "version-2",
			},
			options: RedisOptions{
				EngineVersion: "version-3",
			},
			expectedInstance: &RedisInstance{
				EngineVersion: "version-3",
			},
			expectUpdates: true,
		},
		"sets tags": {
			existingInstance: &RedisInstance{},
			newPlan: catalog.RedisPlan{
				Tags: map[string]string{
					"foo": "bar",
				},
			},
			tags: map[string]string{
				"foo2": "baz",
			},
			expectedInstance: &RedisInstance{
				Tags: map[string]string{
					"foo":  "bar",
					"foo2": "baz",
				},
			},
			expectUpdates: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			modifiedInstance := test.existingInstance.modify(test.options, &test.newPlan, test.tags)

			if test.expectUpdates {
				if diff := deep.Equal(test.existingInstance, test.expectedInstance); diff == nil {
					t.Error("Expected no modifications to existing instance")
					t.Error(diff)
				}
			}

			if diff := deep.Equal(modifiedInstance, test.expectedInstance); diff != nil {
				t.Fatal(diff)
			}
		})
	}

}

func TestInitInstanceTags(t *testing.T) {
	plan := catalog.RedisPlan{
		Tags: map[string]string{
			"plan-tag-1": "foo",
		},
	}
	tags := map[string]string{
		"tag-1": "bar",
	}

	instance := &RedisInstance{}
	instance.init(
		"uuid-1",
		"org-1",
		"space-1",
		"service-1",
		plan,
		RedisOptions{},
		&config.Settings{
			EncryptionKey: helpers.RandStr(16),
		},
		tags,
	)

	expectedTags := map[string]string{
		"plan-tag-1": "foo",
		"tag-1":      "bar",
	}

	if diff := deep.Equal(instance.Tags, expectedTags); diff != nil {
		t.Error(diff)
	}
}

func TestGetCredentials(t *testing.T) {
	instance := &RedisInstance{
		Instance: base.Instance{
			Host: "host",
			Port: 6379,
		},
		EngineVersion: "5",
	}

	credentials, err := instance.getCredentials("foobar")
	if err != nil {
		t.Fatal(err)
	}
	expectedCredentials := map[string]string{
		"uri":                          "rediss://:foobar@host:6379",
		"password":                     "foobar",
		"host":                         "host",
		"hostname":                     "host",
		"current_redis_engine_version": "5",
		"port":                         "6379",
	}

	if diff := deep.Equal(credentials, expectedCredentials); diff != nil {
		t.Error(diff)
	}
}
