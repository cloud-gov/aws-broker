#!/bin/bash

# Exercise the published customer hardening path against a freshly brokered Oracle
# instance. The SQLcl download is intentionally part of this test: customers use
# the same checksum-verified overlay target before pushing their hardening app.
set -Eeuo pipefail

# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

: "${BROKER_NAME:?BROKER_NAME is required}"
: "${CF_API_URL:?CF_API_URL is required}"
: "${CF_USERNAME:?CF_USERNAME is required}"
: "${CF_PASSWORD:?CF_PASSWORD is required}"
: "${CF_ORGANIZATION:?CF_ORGANIZATION is required}"
: "${CF_SPACE:?CF_SPACE is required}"
: "${ORACLE_STIG_VALIDATOR_IMAGE:?ORACLE_STIG_VALIDATOR_IMAGE is required}"
: "${DOCKER_USER:?DOCKER_USER is required}"
: "${CF_DOCKER_PASSWORD:?CF_DOCKER_PASSWORD is required}"

SERVICE_PLAN="${SERVICE_PLAN:-medium-oracle-se2}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-7200}"
APP_WAIT_TIMEOUT="${APP_WAIT_TIMEOUT:-900}"
TEST_ID=$(get_test_id)
SERVICE_INSTANCE="oracle-stig-smoke-test-${TEST_ID}"
VALIDATOR_APP="oracle-stig-validator-smoke-test-${TEST_ID}"
SQLCL_APP="oracle-stig-sqlcl-smoke-test-${TEST_ID}"

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e

  cf delete -f "$SQLCL_APP"
  cf delete -f "$VALIDATOR_APP"

  if cf service "$SERVICE_INSTANCE" >/dev/null 2>&1; then
    cf delete-service -f "$SERVICE_INSTANCE"
    wait_for_deletion "$SERVICE_INSTANCE"
  fi

  exit "$status"
}

wait_for_app_running() {
  local app_name=$1
  local waited=0
  local state

  while true; do
    state=$(cf app "$app_name" 2>/dev/null | awk '/^instances:/ { print $2 }')
    if [ "$state" = "1/1" ]; then
      return 0
    fi

    if [ "$waited" -ge "$APP_WAIT_TIMEOUT" ]; then
      echo "ERROR: timed out after ${APP_WAIT_TIMEOUT}s waiting for app ${app_name} to start" >&2
      cf app "$app_name"
      return 1
    fi

    sleep 15
    waited=$((waited + 15))
  done
}

bind_service_with_timeout() {
  local app_name=$1
  local service_name=$2
  local waited=0
  local output

  while true; do
    if output=$(cf bind-service "$app_name" "$service_name" 2>&1); then
      return 0
    fi

    if [[ "$output" != *"Instance not available yet"* ]]; then
      echo "$output" >&2
      return 1
    fi

    if [ "$waited" -ge "$APP_WAIT_TIMEOUT" ]; then
      echo "ERROR: timed out after ${APP_WAIT_TIMEOUT}s binding ${service_name} to ${app_name}" >&2
      return 1
    fi

    echo "$output" >&2
    sleep 15
    waited=$((waited + 15))
  done
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in cf curl unzip md5sum sha1sum; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "ERROR: required command not found: $command" >&2
    exit 1
  }
done

for path in \
  cg-oracle-stig-overlay/Makefile \
  cg-oracle-stig-overlay/runner/run-validation.sh \
  cg-oracle-stig-overlay/runner/sqlcl-cf/manifest.yml \
  cg-oracle-stig-overlay/hardening/sql/customer-hardening-input.sql.example; do
  [ -f "$path" ] || {
    echo "ERROR: required overlay file is missing: $path" >&2
    exit 1
  }
done

login

cf create-service aws-rds "$SERVICE_PLAN" "$SERVICE_INSTANCE" -b "$BROKER_NAME"
wait_for_service_instance_success_with_timeout "$SERVICE_INSTANCE" "$WAIT_TIMEOUT"

cf push "$VALIDATOR_APP" --no-start --no-route -u process -k 2GB \
  --docker-image "$ORACLE_STIG_VALIDATOR_IMAGE" --docker-username "$DOCKER_USER" \
  -c 'sleep infinity'
bind_service_with_timeout "$VALIDATOR_APP" "$SERVICE_INSTANCE"
cf start "$VALIDATOR_APP"
wait_for_app_running "$VALIDATOR_APP"

cf ssh "$VALIDATOR_APP" -c \
  '/usr/local/bin/run-validation.sh --skip-customer-controls'

pushd cg-oracle-stig-overlay
# Download and verify SQLcl exactly as a customer following the overlay does.
make vendor-sqlcl-cf stage-sqlcl-cf
cf push "$SQLCL_APP" -f runner/sqlcl-cf/manifest.yml -p runner/sqlcl-cf --no-start
bind_service_with_timeout "$SQLCL_APP" "$SERVICE_INSTANCE"
cf start "$SQLCL_APP"
wait_for_app_running "$SQLCL_APP"

# $HOME must expand in the remote CF container.
# shellcheck disable=SC2016
cf ssh "$SQLCL_APP" -c \
  '$HOME/app/entrypoint.sh -f $HOME/app/hardening/00_connectivity_check.sql'

make harden-customer-cloudgov \
  SQLCL_CF_APP="$SQLCL_APP" \
  CF_APP_INSTANCE=0 \
  HARDENING_INPUT=hardening/sql/customer-hardening-input.sql.example
popd

cf ssh "$VALIDATOR_APP" -c '/usr/local/bin/run-validation.sh --all'
