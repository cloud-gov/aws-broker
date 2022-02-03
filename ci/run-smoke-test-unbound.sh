#!/bin/bash

set -euxo pipefail


# Environment variables usered for reference
# $CF_API_URL
# $CF_USERNAME
# $CF_PASSWORD
# $CF_ORGANIZATION
# $CF_SPACE
# $SERVICE_NAME - ie. aws-rds, aws-elasticache-redis
# $SERVICE_PLAN - ie. micro-psql, small-mysql, redis-dev
# $REGION which region service instance is running in, for ES

# Computed vars
TEST_SERVICE="smoke-test-$SERVICE_PLAN-unbound-service"

# Function for waiting on a service instance to finish being processed.
wait_for_service_instance() {
  local service_instance_name=$1
  local guid=$(cf service --guid $service_instance_name)
  local state=$(cf curl /v3/service_instances/${guid} | jq -r '.last_operation.state')

  while [ "$state" == "in progress" ]; do
    echo "Still waiting"
    sleep 120
    state=$(cf curl /v3/service_instances/${guid} | jq -r '.last_operation.state')
  done
}

# Function for getting task state
get_task_state() {
  local app_guid=$1
  local task_state=$(cf curl "/v3/tasks?app_guids=$app_guid&order_by=-created_at" | jq -r ".resources[0].state")

  while [ "$task_state" != "FAILED" ] && [ "$task_state" != "SUCCEEDED" ]; do
    sleep 15
    task_state=$(cf curl "/v3/tasks?app_guids=$app_guid&order_by=-created_at" | jq -r ".resources[0].state")
  done

  # If task FAILED exit with error
  if [[ "$task_state" == "FAILED" ]]; then
    echo "Smoke test failed."
    echo "Check '$> cf logs $TEST_APP --recent' for more info."
    exit 1
  fi

  echo "$task_state"
}

# Log into CF
cf login -a "$CF_API_URL" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$CF_ORGANIZATION" -s "$CF_SPACE"

# Clean up existing app and service if present
cf delete-service -f $TEST_SERVICE

# Create service
cf create-service $SERVICE_NAME $SERVICE_PLAN $TEST_SERVICE

# Wait for service to be created
wait_for_service_instance $TEST_SERVICE

# now delete service
cf delete-service -f $TEST_SERVICE

# Wait for service to be deleted
wait_for_service_instance $TEST_SERVICE