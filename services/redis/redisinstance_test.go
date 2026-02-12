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

func TestInitInstance(t *testing.T) {
	plan := catalog.RedisPlan{
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
			EncryptionKey:     helpers.RandStr(16),
			DbShorthandPrefix: "prefix",
		},
		tags,
	)

	expectedInstance := &RedisInstance{
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
	}

	if diff := deep.Equal(instance, expectedInstance); diff != nil {
		t.Error(diff)
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
