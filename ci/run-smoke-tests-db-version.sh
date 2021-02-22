#!/bin/bash

set -euxo pipefail

# Function for waiting on a service instance to finish being processed.
wait_for_service_instance() {
  local service_name=$1
  local guid=$(cf service --guid $service_name)
  local status=$(cf curl /v2/service_instances/${guid} | jq -r '.entity.last_operation.state')

  while [ "$status" == "in progress" ]; do
    sleep 60
    status=$(cf curl /v2/service_instances/${guid} | jq -r '.entity.last_operation.state')
  done
}

# todo (mxplusb): update the auth mechanism.
cf login -a "$CF_API_URL" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$CF_ORGANIZATION" -s "$CF_SPACE"

# Clean up existing app and service if present
cf delete -f "smoke-tests-db-version-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-db-version-$SERVICE_PLAN"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "smoke-tests-db-version-${SERVICE_PLAN}" -f manifest.yml --no-start

# set some variables that it needs
cf set-env "smoke-tests-db-version-${SERVICE_PLAN}" DB_TYPE "${SERVICE_PLAN}"
cf set-env "smoke-tests-db-version-${SERVICE_PLAN}" SERVICE_NAME "rds-smoke-tests-db-version-$SERVICE_PLAN"

# Create service
if echo "$SERVICE_PLAN" | grep -v shared | grep mysql >/dev/null ; then
  # test out the enable_functions stuff, which only works on non-shared mysql databases
  cf create-service aws-rds "$SERVICE_PLAN" "rds-smoke-tests-db-version-$SERVICE_PLAN" -c '{"enable_functions": true, "version": "'"$VERSION"'"}'
else
  # create a regular instance
  cf create-service aws-rds "$SERVICE_PLAN" "rds-smoke-tests-db-version-$SERVICE_PLAN" -c '{"version": "'"$VERSION"'"}'
fi

while true; do
  if out=$(cf bind-service "smoke-tests-db-version-${SERVICE_PLAN}" "rds-smoke-tests-db-version-$SERVICE_PLAN"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "smoke-tests-db-version-${SERVICE_PLAN}"

# Clean up app and service
cf delete -f "smoke-tests-db-version-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-db-version-$SERVICE_PLAN"
