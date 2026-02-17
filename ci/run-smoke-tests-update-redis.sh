#!/bin/bash

set -euxo pipefail

. aws-broker-app/ci/ci-utils.sh

# Log in to CF
login

TEST_ID="$RANDOM"
SERVICE_NAME="rds-smoke-tests-db-update-$SERVICE_PLAN-$TEST_ID"

# Clean up existing app and service if present
cf delete -f "smoke-tests-db-update-$SERVICE_PLAN"
cf delete-service -f "$SERVICE_NAME"

# Create service
cf create-service aws-elasticache-redis $SERVICE_PLAN $SERVICE_NAME -b "$BROKER_NAME"

# Wait for service to be created
wait_for_service_instance $TEST_SERVICE

# Update service
cf update-service "$SERVICE_NAME" -p "$NEW_SERVICE_PLAN"

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Clean up app and service
cf delete-service -f $TEST_SERVICE

# Wait for service to be deleted
wait_for_deletion "$SERVICE_NAME"
