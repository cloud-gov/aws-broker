#!/bin/bash

set -euxo pipefail

# todo (mxplusb): update the auth mechanism.
cf login -a "$CF_API_URL" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$CF_ORGANIZATION" -s "$CF_SPACE"

# Clean up existing app and service if present
cf delete -f "smoke-tests-pg-version-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-pg-version-$SERVICE_PLAN"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "smoke-tests-pg-version-${SERVICE_PLAN}" -f manifest.yml --no-start

# set some variables that it needs
cf set-env "smoke-tests-pg-version-${SERVICE_PLAN}" DB_TYPE "${SERVICE_PLAN}"
cf set-env "smoke-tests-pg-version-${SERVICE_PLAN}" SERVICE_NAME "rds-smoke-tests-pg-version-$SERVICE_PLAN"

# Create service
cf create-service aws-rds "$SERVICE_PLAN" "rds-smoke-tests-pg-version-$SERVICE_PLAN" -c '{"version": 10}'

while true; do
  if out=$(cf bind-service "smoke-tests-pg-version-${SERVICE_PLAN}" "rds-smoke-tests-pg-version-$SERVICE_PLAN"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "smoke-tests-pg-version-${SERVICE_PLAN}"

# Clean up app and service
cf delete -f "smoke-tests-pg-version-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-pg-version-$SERVICE_PLAN"
