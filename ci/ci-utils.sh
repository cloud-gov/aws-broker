#!/bin/bash

login() {
  cf api "$CF_API_URL"
  set +x
  cf auth "$CF_USERNAME" "$CF_PASSWORD"
  set -x
  cf target -o "$CF_ORGANIZATION" -s "$CF_SPACE"
}

# Function for waiting on a service instance to finish being processed.
wait_for_service_instance() {
  local service_name=$1
  local guid=$(cf service --guid $service_name)
  local status=$(cf curl /v2/service_instances/${guid} | jq -r '.entity.last_operation.state')

  while [ "$status" == "in progress" ]; do
    sleep 60
    status=$(cf curl /v2/service_instances/${guid} | jq -r '.entity.last_operation.state')
  done
}

function wait_for_deletion {
  while true; do
    if ! cf service "$1"; then
      break
    fi
    echo "Waiting for $1 to be deleted"
    sleep 90
  done
}

wait_for_service_bindable() {
  while true; do
    if out=$(cf bind-service "$1" "$2"); then
      break
    fi
    if [[ $out =~ "Instance not available yet" ]]; then
      echo "${out}"
    fi
    sleep 90
  done
}
