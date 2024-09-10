#!/bin/bash

set -euxo pipefail


# Function for waiting on a service instance to finish being processed.
wait_for_service_instance() {
  local service_name=$1
  local guid=$(cf service --guid $service_name)
  local status=$(cf curl /v2/service_instances/${guid} | jq -r '.entity.last_operation.state')

  while [ "$status" == "in progress" ]; do
    sleep 30
    status=$(cf curl /v2/service_instances/${guid} | jq -r '.entity.last_operation.state')
  done
}

cf login -a "$CF_API_URL" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$CF_ORGANIZATION" -s "$CF_SPACE"

# Clean up existing app and service if present
cf delete -f "smoke-tests-db-rotate-creds-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" -f manifest.yml --var rds-service="rds-smoke-tests-$SERVICE_PLAN" --no-start

# set some variables that it needs
cf set-env "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" DB_TYPE "${SERVICE_PLAN}"
cf set-env "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" SERVICE_NAME "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"

# Create service instance
cf create-service aws-rds "$SERVICE_PLAN" "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN" -b "$BROKER_NAME"

while true; do
  if out=$(cf bind-service "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "smoke-tests-db-rotate-creds-${SERVICE_PLAN}"

# Rotate creds
cf update-service "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN" -c '{"rotate_credentials": true}'

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"

# Unbind and re-bind service to get new credentials
cf unbind-service "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"
while true; do
  if out=$(cf bind-service "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# Restage app with new credentials
cf restage "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" --var rds-service="rds-smoke-tests-$SERVICE_PLAN"

# Restart app - if it succeeds, then smoke tests have passed with new credentials
cf restart "smoke-tests-db-rotate-creds-${SERVICE_PLAN}"

# Clean up app and service
cf delete -f "smoke-tests-db-rotate-creds-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"
