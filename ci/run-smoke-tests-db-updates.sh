#!/bin/bash

set -euxo pipefail

. aws-broker-app/ci/ci-utils.sh

# Log in to CF
login

TEST_ID="$RANDOM"
APP_NAME="smoke-tests-db-update-$SERVICE_PLAN-$TEST_ID"
SERVICE_NAME="rds-smoke-tests-db-update-$SERVICE_PLAN-$TEST_ID"
OLD_VERSION=${OLD_VERSION:-""}
NEW_VERSION=${NEW_VERSION:-""}
NEW_SERVICE_PLAN=${NEW_SERVICE_PLAN:-""}
NEW_STORAGE=${NEW_STORAGE:-""}

# Clean up existing app and service if present
cf delete -f "smoke-tests-db-update-$SERVICE_PLAN"
cf delete-service -f "$SERVICE_NAME"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "$APP_NAME" -f manifest.yml --var rds-service="$SERVICE_NAME" --no-start

# set some variables that it needs
cf set-env "$APP_NAME" DB_TYPE "$DB_TYPE"
cf set-env "$APP_NAME" SERVICE_NAME "$SERVICE_NAME"

# Create service
create_service_args=(aws-rds "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME")

if [ -n "$OLD_VERSION" ]; then
  create_service_args+=(-c '{"version": "'"$OLD_VERSION"'"}')
fi

cf create-service "${create_service_args[@]}"

wait_for_service_bindable "$APP_NAME" "$SERVICE_NAME"

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "$APP_NAME" --var rds-service="$SERVICE_NAME"

# Update service
update_service_args=("$SERVICE_NAME")

if [ -n "$NEW_SERVICE_PLAN" ]; then
  update_service_args+=(-p "$NEW_SERVICE_PLAN")
fi

if [ -n "$NEW_VERSION" ]; then
  update_service_args+=(-c '{"version": "'"$NEW_VERSION"'"}')
fi

if [ -n "$NEW_STORAGE" ]; then
  update_service_args+=(-c "{\"version\": $NEW_STORAGE}")
fi

cf update-service "${update_service_args[@]}"

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
wait_for_deletion "$SERVICE_NAME"
