#!/bin/bash

set -euxo pipefail

function wait_for_deletion {
  while true; do
    if ! cf service "$1"; then
      break
    fi
    echo "Waiting for $1 to be deleted"
    sleep 90
  done
}

APP_NAME="smoke-tests-$SERVICE_PLAN"
SERVICE_NAME="rds-smoke-tests-$SERVICE_PLAN"

# todo (mxplusb): update the auth mechanism.
cf login -a "$CF_API_URL" -u "$CF_USERNAME" -p "$CF_PASSWORD" -o "$CF_ORGANIZATION" -s "$CF_SPACE"

# Clean up existing app and service if present
cf delete -f "$APP_NAME"

if cf service "$SERVICE_NAME"; then
  cf delete-service -f "$SERVICE_NAME"
  wait_for_deletion "$SERVICE_NAME"
fi

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "smoke-tests-${SERVICE_PLAN}" -f manifest.yml --var rds-service="$SERVICE_NAME" --no-start

# set some variables that it needs
cf set-env "smoke-tests-${SERVICE_PLAN}" DB_TYPE "${SERVICE_PLAN}"
cf set-env "smoke-tests-${SERVICE_PLAN}" SERVICE_NAME "$SERVICE_NAME"

# Create service
if echo "$SERVICE_PLAN" | grep mysql >/dev/null ; then
  # test out the enable_functions stuff
  cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME" -c '{"enable_functions": true}'
else
  # create a regular instance
  cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME"
fi

while true; do
  if out=$(cf bind-service "smoke-tests-${SERVICE_PLAN}" "$SERVICE_NAME"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "smoke-tests-${SERVICE_PLAN}" --var rds-service="$SERVICE_NAME"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
wait_for_deletion "$SERVICE_NAME"
