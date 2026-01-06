#!/bin/bash

set -euxo pipefail

. ./ci-utils.sh

# Log in to CF
login

APP_NAME="smoke-tests-db-rotate-creds-$SERVICE_PLAN"
SERVICE_NAME="rds-smoke-tests-db-rotate-creds-$SERVICE_PLAN"

# Clean up existing app and service if present
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "smoke-tests-db-rotate-creds-${SERVICE_PLAN}" -f manifest.yml --var rds-service="$SERVICE_NAME" --no-start

# set some variables that it needs
cf set-env "$APP_NAME" DB_TYPE "$DB_TYPE"
cf set-env "$APP_NAME" SERVICE_NAME "$SERVICE_NAME"

# Create service instance
cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME"

while true; do
  if out=$(cf bind-service "$APP_NAME" "$SERVICE_NAME"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "$APP_NAME" --var rds-service="$SERVICE_NAME"

# Rotate creds
cf update-service "$SERVICE_NAME" -c '{"rotate_credentials": true}'

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Unbind and re-bind service to get new credentials
cf unbind-service "$APP_NAME" "$SERVICE_NAME"
while true; do
  if out=$(cf bind-service "$APP_NAME" "$SERVICE_NAME"); then
    break
  fi
  if [[ $out =~ "Instance not available yet" ]]; then
    echo "${out}"
  fi
  sleep 90
done

# Restage app with new credentials
cf restage "$APP_NAME"

# Restart app - if it succeeds, then smoke tests have passed with new credentials
cf restart "$APP_NAME"

# Clean up app and service
cf delete -f "smoke-tests-db-rotate-creds-$SERVICE_PLAN"
cf delete-service -f "$SERVICE_NAME"
