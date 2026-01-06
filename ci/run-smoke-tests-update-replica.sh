#!/bin/bash

set -euxo pipefail

. ./ci-utils.sh

APP_NAME="smoke-tests-db-update-replica-$SERVICE_PLAN"
SERVICE_NAME="rds-smoke-tests-db-update-replica-$SERVICE_PLAN"

# Clean up existing app and service if present
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"

# change into the directory and push the app without starting it.
pushd aws-db-test/databases/aws-rds
cf push "$APP_NAME" -f manifest.yml --var rds-service="$SERVICE_NAME" --no-start

# Create service
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

# Update service to replica plan
cf update-service "$SERVICE_NAME" -p "$REPLICA_PLAN"

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance "$SERVICE_NAME"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
