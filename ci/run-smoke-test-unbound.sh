#!/bin/bash

set -euxo pipefail

. aws-broker-app/ci/ci-utils.sh

# Environment variables usered for reference
# $CF_API_URL
# $CF_USERNAME
# $CF_PASSWORD
# $CF_ORGANIZATION
# $CF_SPACE
# $SERVICE_NAME - ie. aws-rds, aws-elasticache-redis
# $SERVICE_PLAN - ie. micro-psql, small-mysql, redis-dev
# $REGION which region service instance is running in, for ES

# Computed vars
TEST_SERVICE="smoke-test-$SERVICE_PLAN-unbound-service"

# Log into CF
login

# Clean up existing app and service if present
cf delete-service -f $TEST_SERVICE

# Wait for service to be created
wait_for_service_instance $TEST_SERVICE

# Create service
cf create-service $SERVICE_NAME $SERVICE_PLAN $TEST_SERVICE -b "$BROKER_NAME"

# Wait for service to be created
wait_for_service_instance $TEST_SERVICE

# now delete service
cf delete-service -f $TEST_SERVICE

# Wait for service to be deleted
wait_for_deletion "$SERVICE_NAME"
