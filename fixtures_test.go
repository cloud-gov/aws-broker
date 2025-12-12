package main

import "fmt"

var (
	rdsServiceId                = "db80ca29-2d1b-4fbc-aad3-d03c0bfa7593"
	originalRDSPlanID           = "da91e15c-98c9-46a9-b114-02b8d28062c6"
	updateableRDSPlanID         = "1070028c-b5fb-4de8-989b-4e00d07ef5e8"
	originalRedisPlanID         = "475e36bf-387f-44c1-9b81-575fec2ee443"
	originalElasticsearchPlanID = "55b529cf-639e-4673-94fd-ad0a5dafe0ad"
)

// micro-psql plan
var createRDSInstanceReq = []byte(
	fmt.Sprintf(`{
	"service_id":"%s",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`, rdsServiceId))

var createRDSInstanceWithEnabledLogGroupsReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"parameters": {
	  "enable_cloudwatch_log_groups_exports": ["foo"]
	}
}`)

var createRDSPGWithVersionInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"version": "15"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSPGWithInvaildVersionInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"version": "8"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSMySQLWithBinaryLogFormat = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"binary_log_format": "ROW"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var createRDSPostgreSQLWithEnablePgCron = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"enable_pg_cron": true
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

// micro-psql plan but with parameters
var modifyRDSInstanceReqStorage = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"storage": 25
	  },
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRDSInstanceBinaryLogFormat = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"binary_log_format": "MIXED"
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRDSInstanceEnablePgCron = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"enable_pg_cron": true
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRDSInstanceEnableCloudwatchLogGroups = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"da91e15c-98c9-46a9-b114-02b8d28062c6",
	"parameters": {
		"enable_cloudwatch_log_groups_exports": ["foo"]
	},
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

// medium-psql plan
var modifyRDSInstanceReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"1070028c-b5fb-4de8-989b-4e00d07ef5e8",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "da91e15c-98c9-46a9-b114-02b8d28062c6"
	}
}`)

// medium-psql-redundant plan
var modifyRDSInstanceNotAllowedReq = []byte(
	`{
	"service_id":"db80ca29-2d1b-4fbc-aad3-d03c0bfa7593",
	"plan_id":"ee75aef3-7697-4906-9330-fb1f83d719be",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "da91e15c-98c9-46a9-b114-02b8d28062c6"
	}
}`)

var createRedisInstanceReq = []byte(
	`{
	"service_id":"cda65825-e357-4a93-a24b-9ab138d97815",
	"plan_id":"475e36bf-387f-44c1-9b81-575fec2ee443",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyRedisInstanceReq = []byte(
	`{
	"service_id":"cda65825-e357-4a93-a24b-9ab138d97815",
	"plan_id":"5nd336bf-0k7f-44c1-9b81-575fp3k764r6",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "475e36bf-387f-44c1-9b81-575fec2ee443"
	}
}`)

var createElasticsearchInstanceAdvancedOptionsReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"55b529cf-639e-4673-94fd-ad0a5dafe0ad",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"parameters": {
		"advanced_options": {
			"indices.query.bool.max_clause_count": "1024",
			"indices.fielddata.cache.size": "80"
		}
	}
}`)

var createElasticsearchInstanceReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"55b529cf-639e-4673-94fd-ad0a5dafe0ad",
	"organization_guid":"an-org",
	"space_guid":"a-space"
}`)

var modifyElasticsearchInstancePlanReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"162ffae8-9cf8-4806-80e5-a7f92d514198",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"previous_values": {
		"plan_id": "55b529cf-639e-4673-94fd-ad0a5dafe0ad"
	}
}`)

var modifyElasticsearchInstanceParamsReq = []byte(
	`{
	"service_id":"90413816-9c77-418b-9fc7-b9739e7c1254",
	"plan_id":"55b529cf-639e-4673-94fd-ad0a5dafe0ad",
	"organization_guid":"an-org",
	"space_guid":"a-space",
	"parameters": {
		"advanced_options": {
			"indices.query.bool.max_clause_count": "1024",
			"indices.fielddata.cache.size": "80"
		}
	}
}`)
