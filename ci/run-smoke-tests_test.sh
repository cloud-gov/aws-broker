#!/bin/bash

set -uo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT

ln -s "$repo_root" "$test_dir/aws-broker-app"

bind_marker="$test_dir/bind-attempted"
curl_count_file="$test_dir/curl-count"
service_state=failed
export bind_marker curl_count_file service_state

cf() {
  case "$1" in
  service)
    if [ "${2:-}" = "--guid" ]; then
      echo "failed-oracle-service-guid"
      return 0
    fi
    return 1
    ;;
  curl)
    if [ "$service_state" = "timeout" ]; then
      local curl_count=0
      if [ -f "$curl_count_file" ]; then
        curl_count=$(<"$curl_count_file")
      fi
      curl_count=$((curl_count + 1))
      printf '%s' "$curl_count" >"$curl_count_file"
      printf '%s\n' '{"last_operation":{"state":"in progress"}}'
      return 0
    fi
    printf '%s\n' '{"last_operation":{"state":"failed"}}'
    ;;
  bind-service)
    touch "$bind_marker"
    return 0
    ;;
  *)
    return 0
    ;;
  esac
}
export -f cf

export BROKER_NAME=test-broker
export CF_API_URL=https://api.example.invalid
export CF_USERNAME=test-user
export CF_PASSWORD=test-password
export CF_ORGANIZATION=test-org
export CF_SPACE=test-space
export SERVICE_PLAN=medium-oracle-se2
export DB_TYPE=oracle-se2

status=0
(
  cd "$test_dir" || exit 1
  bash aws-broker-app/ci/run-smoke-tests.sh
) >/dev/null 2>&1 || status=$?

if [ "$status" -eq 0 ]; then
  echo "FAIL: Oracle smoke test accepted failed service provisioning"
  exit 1
fi

if [ -e "$bind_marker" ]; then
  echo "FAIL: Oracle smoke test attempted to bind after failed provisioning"
  exit 1
fi

echo "PASS: Oracle smoke test rejects failed service provisioning before binding"

rm -f "$bind_marker" "$curl_count_file"
service_state=timeout
export service_state

sleep() {
  :
}
export -f sleep

status=0
(
  cd "$test_dir" || exit 1
  bash aws-broker-app/ci/run-smoke-tests.sh
) >/dev/null 2>&1 || status=$?

if [ "$status" -eq 0 ]; then
  echo "FAIL: Oracle smoke test accepted timed-out service provisioning"
  exit 1
fi

if [ -e "$bind_marker" ]; then
  echo "FAIL: Oracle smoke test attempted to bind after provisioning timed out"
  exit 1
fi

if [ "$(<"$curl_count_file")" -ne 61 ]; then
  echo "FAIL: Oracle smoke test did not use the expected 3600-second timeout"
  exit 1
fi

echo "PASS: Oracle smoke test times out provisioning after 3600 seconds before binding"
