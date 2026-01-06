#!/bin/bash

set -euxo pipefail

. ./ci-utils.sh

# Log in to CF
login

APP_NAME="smoke-tests-db-update-storage-$SERVICE_PLAN"
SERVICE_NAME="rds-smoke-tests-db-update-storage-$SERVICE_PLAN"

# Clean up existing app and service if present
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "$APP_NAME" -f manifest.yml --var rds-service="$SERVICE_NAME" --no-start

# set some variables that it needs
cf set-env "$APP_NAME" DB_TYPE "$DB_TYPE"
cf set-env "$APP_NAME" SERVICE_NAME "$SERVICE_NAME"

# Create service
cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME"

wait_for_service_bindable $APP_NAME $SERVICE_NAME

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "$APP_NAME" --var rds-service="$SERVICE_NAME"

# Update storage size
cf update-service "$SERVICE_NAME" -c '{"storage": 25}'

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
