#!/bin/bash

set -euxo pipefail

. aws-broker-app/ci/ci-utils.sh

# Log in to CF
login

TEST_ID="$RANDOM"
SERVICE_NAME="redis-smoke-tests-update-version-$SERVICE_PLAN-$TEST_ID"

# Clean up existing service if present
cf delete-service -f "$SERVICE_NAME"

# Create service
cf create-service aws-elasticache-redis $SERVICE_PLAN $SERVICE_NAME -b "$BROKER_NAME" -c '{"engine_version": "'"$OLD_VERSION"'"}'

# Wait for service to be created
wait_for_service_instance $SERVICE_NAME

# Update service
cf update-service "$SERVICE_NAME" -c '{"engine_version": "'"$NEW_VERSION"'"}'

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Clean up service
cf delete-service -f $SERVICE_NAME

# Wait for service to be deleted
wait_for_deletion "$SERVICE_NAME"
