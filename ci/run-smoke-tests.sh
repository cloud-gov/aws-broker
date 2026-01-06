#!/bin/bash

set -euxo pipefail

. ./ci-utils.sh

APP_NAME="smoke-tests-$SERVICE_PLAN"
SERVICE_NAME="rds-smoke-tests-$SERVICE_PLAN"

# Log in to CF
login

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
cf set-env "smoke-tests-${SERVICE_PLAN}" DB_TYPE "$DB_TYPE"
cf set-env "smoke-tests-${SERVICE_PLAN}" SERVICE_NAME "$SERVICE_NAME"

# Create service
if echo "$SERVICE_PLAN" | grep mysql >/dev/null ; then
  # test out the enable_functions stuff
  cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME" -c '{"enable_functions": true}'
else
  # create a regular instance
  cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME"
fi

wait_for_service_bindable $APP_NAME $SERVICE_NAME

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "$APP_NAME" --var rds-service="$SERVICE_NAME"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
wait_for_deletion "$SERVICE_NAME"
