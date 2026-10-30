package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/cloud-gov/aws-broker/services/redis"
	"github.com/google/uuid"
)

// seedLegacyRedis creates an instance then rewrites it in the DB to look like
// one provisioned before the switch to Valkey.
func seedLegacyRedis(t *testing.T, version string) string {
	t.Helper()
	instanceUUID := uuid.NewString()
	createURL := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", instanceUUID)
	res := requestHandler.doRequest(createURL, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Fatalf("seed create failed: %d %s", res.Code, res.Body.String())
	}
	err := brokerDB.Model(&redis.RedisInstance{}).
		Where("uuid = ?", instanceUUID).
		Updates(map[string]interface{}{"engine": "redis", "engine_version": version}).Error
	if err != nil {
		t.Fatal(err)
	}
	return instanceUUID
}

func legacyInstance(t *testing.T, instanceUUID string) redis.RedisInstance {
	t.Helper()
	i := redis.RedisInstance{}
	brokerDB.Where("uuid = ?", instanceUUID).First(&i)
	return i
}

// 1. A legacy Redis instance can be read back / polled.
func TestLegacyRedisLastOperation(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s/last_operation?service_id=%s&plan_id=%s", u, redisServiceId, originalRedisPlanID)
	res := requestHandler.doRequest(url, "GET", true, nil)
	if res.Code != http.StatusOK {
		t.Errorf("last_operation: want 200, got %d: %s", res.Code, res.Body.String())
	}
}

// 2. A legacy Redis instance can be bound, and credentials report its version.
func TestLegacyRedisBind(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/b1?service_id=%s&plan_id=%s", u, redisServiceId, originalRedisPlanID)
	res := requestHandler.doRequest(url, "PUT", true, bytes.NewBuffer(createRedisInstanceReq))
	if res.Code != http.StatusCreated {
		t.Fatalf("bind: want 201, got %d: %s", res.Code, res.Body.String())
	}
	var r struct {
		Credentials map[string]string
	}
	json.Unmarshal(res.Body.Bytes(), &r) //nolint:errcheck
	if got := r.Credentials["current_redis_engine_version"]; got != "7.0" {
		t.Errorf("bind credentials: want version 7.0, got %q", got)
	}
	if r.Credentials["uri"] == "" {
		t.Error("bind credentials: missing uri")
	}
}

// 3. A legacy Redis instance can be unbound.
func TestLegacyRedisUnbind(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s/service_bindings/b1?service_id=%s&plan_id=%s", u, redisServiceId, originalRedisPlanID)
	if res := requestHandler.doRequest(url, "PUT", true, bytes.NewBuffer(createRedisInstanceReq)); res.Code != http.StatusCreated {
		t.Fatalf("bind: %d", res.Code)
	}
	res := requestHandler.doRequest(url, "DELETE", true, nil)
	if res.Code != http.StatusOK {
		t.Errorf("unbind: want 200, got %d: %s", res.Code, res.Body.String())
	}
}

// 4. A legacy Redis instance can be upgraded within the Redis engine, 7.0 -> 7.1.
func TestLegacyRedisVersionUpgrade(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", u)
	body := []byte(fmt.Sprintf(`{"service_id":"%s","plan_id":"%s","parameters":{"engine_version":"7.1"}}`, redisServiceId, originalRedisPlanID))
	res := requestHandler.doRequest(url, "PATCH", true, bytes.NewBuffer(body))
	if res.Code != http.StatusAccepted {
		t.Fatalf("redis 7.0->7.1: want 202, got %d: %s", res.Code, res.Body.String())
	}
	i := legacyInstance(t, u)
	if i.Engine != "redis" || i.EngineVersion != "7.1" {
		t.Errorf("want redis/7.1, got %s/%s", i.Engine, i.EngineVersion)
	}
}

// 5. A legacy Redis instance can change plans and keeps Redis.
func TestLegacyRedisPlanChange(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", u)
	res := requestHandler.doRequest(url, "PATCH", true, bytes.NewBuffer(modifyRedisInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Fatalf("plan change: want 202, got %d: %s", res.Code, res.Body.String())
	}
	i := legacyInstance(t, u)
	if i.PlanID != updateableRedisPlanID {
		t.Errorf("plan not changed: %s", i.PlanID)
	}
	if i.Engine != "redis" || i.EngineVersion != "7.0" {
		t.Errorf("want redis/7.0 retained, got %s/%s", i.Engine, i.EngineVersion)
	}
}

// 6. A legacy Redis instance can explicitly opt in to Valkey.
func TestLegacyRedisMigrateToValkey(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", u)
	body := []byte(fmt.Sprintf(`{"service_id":"%s","plan_id":"%s","parameters":{"engine":"valkey","engine_version":"8.2"}}`, redisServiceId, originalRedisPlanID))
	res := requestHandler.doRequest(url, "PATCH", true, bytes.NewBuffer(body))
	if res.Code != http.StatusAccepted {
		t.Fatalf("redis->valkey: want 202, got %d: %s", res.Code, res.Body.String())
	}
	i := legacyInstance(t, u)
	if i.Engine != "valkey" || i.EngineVersion != "8.2" {
		t.Errorf("want valkey/8.2, got %s/%s", i.Engine, i.EngineVersion)
	}
}

// 7. A legacy Redis instance can be deleted.
func TestLegacyRedisDelete(t *testing.T) {
	u := seedLegacyRedis(t, "7.0")
	url := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true&service_id=%s&plan_id=%s", u, redisServiceId, originalRedisPlanID)
	res := requestHandler.doRequest(url, "DELETE", true, nil)
	if res.Code != http.StatusAccepted {
		t.Errorf("delete: want 202, got %d: %s", res.Code, res.Body.String())
	}
}

// 8. A legacy Redis instance on a now-unapproved version can still change plans.
func TestLegacyRedisUnapprovedVersionPlanChange(t *testing.T) {
	u := seedLegacyRedis(t, "6.2")
	url := fmt.Sprintf("/v2/service_instances/%s?accepts_incomplete=true", u)
	res := requestHandler.doRequest(url, "PATCH", true, bytes.NewBuffer(modifyRedisInstanceReq))
	if res.Code != http.StatusAccepted {
		t.Fatalf("unapproved-version plan change: want 202, got %d: %s", res.Code, res.Body.String())
	}
	i := legacyInstance(t, u)
	if i.Engine != "redis" || i.EngineVersion != "6.2" {
		t.Errorf("want redis/6.2 retained, got %s/%s", i.Engine, i.EngineVersion)
	}
}
