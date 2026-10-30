#!/bin/bash

set -euxo pipefail

# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

# login in to CF
login

# Delete leftover apps
cf apps | grep smoke-test | awk '{print $1}' | xargs -I {} cf delete -f {}

echo "All leftover smoke test apps have been deleted."

# Delete leftover services
wait_deletion_pids=()
for service in $(cf services | grep smoke-test | awk '{print $1}'); do
  cf delete-service -f "$service"
  wait_for_deletion "$service" &
  wait_deletion_pids+=($!)
done

# Wait for all service deletions to finish
wait "${wait_deletion_pids[@]}"

echo "All leftover smoke test services have been deleted."
