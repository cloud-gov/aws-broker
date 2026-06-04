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
ALLOW_MAJOR_VERSION_UPGRADE=${ALLOW_MAJOR_VERSION_UPGRADE:-""}
ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS=${ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS:-""}
ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS_ON_CREATE=${ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS_ON_CREATE:-""}
LONG_QUERY_TIME=${LONG_QUERY_TIME:-""}
PG_QUERY_LOGGING=${PG_QUERY_LOGGING:-""}

# Clean up existing app and service if present
cf delete -f "smoke-tests-db-update-$SERVICE_PLAN"
delete_existing_service "$SERVICE_NAME"

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

if [ -n "$ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS_ON_CREATE" ]; then
  create_service_args+=(-c "{\"enable_cloudwatch_log_groups_exports\": $ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS_ON_CREATE}")
fi

cf create-service "${create_service_args[@]}"

# Wait to make sure that the service instance has been successfully created.
wait_for_service_instance_success "$SERVICE_NAME"

# Wait to make sure that the service instance is bound to the application.
wait_for_service_bindable "$APP_NAME" "$SERVICE_NAME"

# wait for the app to start. if the app starts, it's passed the smoke test.
cf push "$APP_NAME" --var rds-service="$SERVICE_NAME"

# Update service
update_service_args=("$SERVICE_NAME")

if [ -n "$NEW_SERVICE_PLAN" ]; then
  update_service_args+=(-p "$NEW_SERVICE_PLAN")
fi

if [ -n "$NEW_VERSION" ]; then
  if [ -n "$ALLOW_MAJOR_VERSION_UPGRADE" ]; then
    update_service_args+=(-c '{"version": "'"$NEW_VERSION"'", "allow_major_version_upgrade": true}')
  else
    update_service_args+=(-c '{"version": "'"$NEW_VERSION"'"}')
  fi
fi

if [ -n "$NEW_STORAGE" ]; then
  update_service_args+=(-c "{\"storage\": $NEW_STORAGE}")
fi

if [ -n "$ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS" ]; then
  update_service_args+=(-c "{\"enable_cloudwatch_log_groups_exports\": $ENABLE_CLOUDWATCH_LOG_GROUP_EXPORTS}")
fi

if [ -n "$LONG_QUERY_TIME" ]; then
  update_service_args+=(-c "{\"long_query_time\": $LONG_QUERY_TIME}")
fi

if [ -n "$PG_QUERY_LOGGING" ]; then
  update_service_args+=(-c "{\"pg_query_logging\": $PG_QUERY_LOGGING}")
fi

cf update-service "${update_service_args[@]}"

# Wait to make sure that the service instance has been successfully updated.
wait_for_service_instance_success "$SERVICE_NAME"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
wait_for_deletion "$SERVICE_NAME"
