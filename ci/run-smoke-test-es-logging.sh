#!/bin/bash

set -euxo pipefail

. aws-broker-app/ci/ci-utils.sh

# Environment variables usered for reference
# $CF_API_URL
# $CF_USERNAME
# $CF_PASSWORD
# $CF_ORGANIZATION
# $CF_SPACE
# $SERVICE_NAME - ie. aws-rds, aws-elasticache-redis
# $SERVICE_PLAN - ie. micro-psql, small-mysql, redis-dev
# $REGION -- which region service is running in (for ES)

# Computed vars
TEST_APP="smoke-test-logging-$SERVICE_PLAN-app"
TEST_SERVICE="smoke-test-logging-$SERVICE_PLAN-service"
TASK_DIRECTORY="aws-broker-app/ci/smoke-tests/$SERVICE_NAME/"

# Log into CF
login

# Clean up existing app and service if present
cf delete -f "$TEST_APP"
delete_existing_service "$TEST_SERVICE"

# change into the directory and push the app without starting it.
pushd "$TASK_DIRECTORY"
cf push "$TEST_APP" -f manifest.yml

cf create-service "$SERVICE_NAME" "$SERVICE_PLAN" "$TEST_SERVICE" -b "$BROKER_NAME" \
    -c '{"log_publishing": {"audit_logs": true, "error_logs": true}}'

# Wait for service to be created
wait_for_service_instance_success "$TEST_SERVICE"

# Bind service to app
wait_for_service_bindable "$TEST_APP" "$TEST_SERVICE"

# Start app
cf restage "$TEST_APP" 

# Run task and verify starting version
cf run-task "$TEST_APP" --command "python run.py -s $TEST_SERVICE -r $REGION"

# Get finished task state with app guid
test_app_guid=$(cf curl "/v3/apps?names=$TEST_APP" | jq -r ".resources[0].guid")
get_task_state "$test_app_guid"

# Upgrade the engine version to target
cf update-service "$TEST_SERVICE" -c '{"log_publishing": {"search_slow_logs": true}}'

# Wait for update to complete
wait_for_service_instance_success "$TEST_SERVICE"

# Re-bind service to app
cf unbind-service "$TEST_APP" "$TEST_SERVICE"
wait_for_service_bindable "$TEST_APP" "$TEST_SERVICE"

# Start app
cf restage "$TEST_APP"

# Run task and verify target version
cf run-task "$TEST_APP" --command "python run.py -s $TEST_SERVICE -r $REGION"

# Get finished task state with app guid
test_app_guid=$(cf curl "/v3/apps?names=$TEST_APP" | jq -r ".resources[0].guid")
get_task_state "$test_app_guid"

# Clean up app and service
cf delete -f "$TEST_APP"
cf delete-service -f "$TEST_SERVICE"

# Wait for service to be deleted
wait_for_deletion "$TEST_SERVICE"
