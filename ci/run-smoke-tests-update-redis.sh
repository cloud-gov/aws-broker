#!/bin/bash

set -euxo pipefail

. aws-broker-app/ci/ci-utils.sh

# Log in to CF
login

TEST_ID="$RANDOM"
SERVICE_NAME="redis-smoke-tests-update-$SERVICE_PLAN-$TEST_ID"

OLD_ENGINE=${OLD_ENGINE:-""}
NEW_ENGINE=${NEW_ENGINE:-""}
OLD_VERSION=${OLD_VERSION:-""}
NEW_VERSION=${NEW_VERSION:-""}
NEW_SERVICE_PLAN=${NEW_SERVICE_PLAN:-""}

# Clean up existing service if present
cf delete-service -f "$SERVICE_NAME"

# Create service
create_service_args=(aws-elasticache-redis "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME")
if [ -n "$OLD_ENGINE" ] && [ -n "$OLD_VERSION" ]; then
  create_service_args+=(-c '{"engine":"'"$OLD_ENGINE"'", "engine_version": "'"$OLD_VERSION"'"}')
elif [ -n "$OLD_VERSION" ]; then
  create_service_args+=(-c '{"engine_version": "'"$OLD_VERSION"'"}')
fi

cf create-service "${create_service_args[@]}"

# Wait for service to be created
wait_for_service_instance "$SERVICE_NAME"

# Update service
update_service_args=("$SERVICE_NAME")

if [ -n "$NEW_SERVICE_PLAN" ]; then
  update_service_args+=(-p "$NEW_SERVICE_PLAN")
fi

if [ -n "$NEW_ENGINE" ] && [ -n "$NEW_VERSION" ]; then
  update_service_args+=(-c '{"engine":"'"$NEW_ENGINE"'", "engine_version": "'"$NEW_VERSION"'"}')
elif [ -n "$NEW_VERSION" ]; then
  update_service_args+=(-c '{"engine_version": "'"$NEW_VERSION"'"}')
fi

cf update-service "${update_service_args[@]}"


# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Clean up service
cf delete-service -f "$SERVICE_NAME"

# Wait for service to be deleted
wait_for_deletion "$SERVICE_NAME"
