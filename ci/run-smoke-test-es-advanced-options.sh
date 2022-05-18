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
# $REGION -- which region service is running in (for ES)

# Computed vars
TEST_APP="smoke-test-adv-$SERVICE_PLAN-app"
TEST_SERVICE="smoke-test-adv-$SERVICE_PLAN-service"
TASK_DIRECTORY="aws-broker-app/ci/smoke-tests/$SERVICE_NAME/"

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
cf delete -f $TEST_APP
cf delete-service -f $TEST_SERVICE

# Wait for service to be deleted
wait_for_service_instance $TEST_SERVICE

# change into the directory and push the app without starting it.
pushd $TASK_DIRECTORY
cf push $TEST_APP -f manifest.yml

# Create service
cf create-service $SERVICE_NAME $SERVICE_PLAN $TEST_SERVICE -b "$BROKER_NAME" -c '{"advanced_options": {"indices.fielddata.cache.size": "21", "indices.query.bool.max_clause_count": "1025"}}'

# Wait for service to be created
wait_for_service_instance $TEST_SERVICE

# Bind service to app
while true; do
  if out=$(cf bind-service $TEST_APP $TEST_SERVICE); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# Start app
cf restage $TEST_APP

# Run task
cf run-task $TEST_APP "python run.py -s $TEST_SERVICE -r $REGION"

# Get finished task state with app guid
test_app_guid=$(cf curl "/v3/apps?names=$TEST_APP" | jq -r ".resources[0].guid")

get_task_state $test_app_guid

# Create service
cf update-service $TEST_SERVICE -c '{"advanced_options": {"indices.fielddata.cache.size": "40", "indices.query.bool.max_clause_count": "1023"}}'

# Wait for service to be created
wait_for_service_instance $TEST_SERVICE

# Bind service to app
while true; do
  if out=$(cf bind-service $TEST_APP $TEST_SERVICE); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# Start app
cf restage $TEST_APP

# Run task
cf run-task $TEST_APP "python run.py -s $TEST_SERVICE -r $REGION"

# Get finished task state with app guid
test_app_guid=$(cf curl "/v3/apps?names=$TEST_APP" | jq -r ".resources[0].guid")

get_task_state $test_app_guid

# Clean up app and service
cf delete -f $TEST_APP
cf delete-service -f $TEST_SERVICE

# Wait for service to be deleted
wait_for_service_instance $TEST_SERVICE