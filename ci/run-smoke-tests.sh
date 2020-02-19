#!/bin/bash

set -euxo pipefail

# todo (mxplusb): update the auth mechanism.
cf login -a "$CF_API_URL" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$CF_ORGANIZATION" -s "$CF_SPACE"

# Clean up existing app and service if present
cf delete -f "smoke-tests-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-$SERVICE_PLAN"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "smoke-tests-${SERVICE_PLAN}" -f manifest.yml --no-start

# set some variables that it needs
cf set-env "smoke-tests-${SERVICE_PLAN}" DB_TYPE "${SERVICE_PLAN}"
cf set-env "smoke-tests-${SERVICE_PLAN}" SERVICE_NAME "rds-smoke-tests-$SERVICE_PLAN"

# Create service
if echo "$SERVICE_PLAN" | grep -v shared | grep mysql >/dev/null ; then
  # test out the enable_functions stuff, which only works on non-shared mysql databases
  cf create-service aws-rds "$SERVICE_PLAN" "rds-smoke-tests-$SERVICE_PLAN" -c '{"enable_functions": true}'
else
  # create a regular instance
  cf create-service aws-rds "$SERVICE_PLAN" "rds-smoke-tests-$SERVICE_PLAN"
fi

while true; do
  if out=$(cf bind-service "smoke-tests-${SERVICE_PLAN}" "rds-smoke-tests-$SERVICE_PLAN"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "smoke-tests-${SERVICE_PLAN}" 

# Clean up app and service
cf delete -f "smoke-tests-$SERVICE_PLAN"
cf delete-service -f "rds-smoke-tests-$SERVICE_PLAN"
