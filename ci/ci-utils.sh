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
  local guid
  # do not exit script if `cf service --guid "$service_name"` fails because
  # instance does not exist
  guid=$(cf service --guid "$service_name" || true)
  local status
  # v3 returns last_operation at the top level of the resource; the `.entity`
  # wrapper was a v2-ism and yields null against v3.
  status=$(cf curl "/v3/service_instances/$guid" | jq -r '.last_operation.state')

  while [ "$status" == "in progress" ]; do
    sleep 60
    status=$(cf curl "/v3/service_instances/$guid" | jq -r '.last_operation.state')
  done

  echo "$status"
}

wait_for_service_instance_success() {
  local status
  status=$(wait_for_service_instance "$1")

  if [ "$status" == "failed" ]; then
    echo "failed to create $1"
    exit 1
  fi
}

# Like wait_for_service_instance_success, but gives up after a bounded number of
# seconds instead of polling forever. Long OpenSearch operations (a plan change is
# a blue/green deployment) can take awhile, tens of minutes add up.
wait_for_service_instance_success_with_timeout() {
  local service_name=$1
  local timeout_seconds=$2
  local interval=60
  local waited=0
  local guid status

  guid=$(cf service --guid "$service_name")

  while true; do
    status=$(cf curl "/v3/service_instances/$guid" | jq -r '.last_operation.state')

    case "$status" in
    succeeded)
      echo "$service_name: $status"
      return 0
      ;;
    failed)
      echo "FAIL: $service_name reported last_operation state 'failed'"
      cf service "$service_name"
      return 1
      ;;
    esac

    if [ "$waited" -ge "$timeout_seconds" ]; then
      echo "FAIL: timed out after ${timeout_seconds}s waiting for $service_name (last state: $status)"
      cf service "$service_name"
      return 1
    fi

    sleep "$interval"
    waited=$((waited + interval))
  done
}

# Assert that a `cf update-service` call is rejected by the broker.
expect_update_service_rejected() {
  local service_name=$1
  local expected_message=$2
  shift 2
  local out

  if out=$(cf update-service "$service_name" "$@" 2>&1); then
    echo "FAIL: expected 'cf update-service $service_name $*' to be rejected, but it was accepted."
    echo "----- output -----"
    echo "$out"
    return 1
  fi

  if ! printf '%s' "$out" | grep -qF -- "$expected_message"; then
    echo "FAIL: expected rejection message to contain:"
    echo "  $expected_message"
    echo "----- actual output -----"
    echo "$out"
    return 1
  fi

  echo "PASS: rejected as expected (matched: $expected_message)"
}

# Assert that a service instance is currently on the expected plan. Used after a
# rejected plan change to prove the broker did not persist the requested plan.
assert_service_plan() {
  local service_name=$1
  local expected_plan=$2
  local plan_guid actual_plan

  plan_guid=$(cf curl "/v3/service_instances?names=$service_name" |
    jq -r '.resources[0].relationships.service_plan.data.guid')
  actual_plan=$(cf curl "/v3/service_plans/$plan_guid" | jq -r '.name')

  if [ "$actual_plan" != "$expected_plan" ]; then
    echo "FAIL: expected $service_name to be on plan '$expected_plan', but it is on '$actual_plan'"
    return 1
  fi

  echo "PASS: $service_name is on plan '$actual_plan'"
}

wait_for_deletion() {
  while true; do
    if ! cf service "$1"; then
      break
    fi
    echo "Waiting for $1 to be deleted"
    sleep 60
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
    sleep 60
  done
}

app_name_from_guid() {
  local app_guid=$1
  local app_name

  app_name=$(cf curl "/v3/apps/$app_guid" 2>/dev/null | jq -r '.name // empty')

  if [ -z "$app_name" ]; then
    echo "$app_guid"
    return 0
  fi

  echo "$app_name"
}

# Function for getting task state
get_task_state() {
  local app_guid=$1
  local task_state
  task_state=$(cf curl "/v3/tasks?app_guids=$app_guid&order_by=-created_at" | jq -r ".resources[0].state")

  while [ "$task_state" != "FAILED" ] && [ "$task_state" != "SUCCEEDED" ]; do
    sleep 15
    task_state=$(cf curl "/v3/tasks?app_guids=$app_guid&order_by=-created_at" | jq -r ".resources[0].state")
  done

  # If task FAILED exit with error
  if [[ "$task_state" == "FAILED" ]]; then
    local app_name
    app_name=$(app_name_from_guid "$app_guid")
    echo "Smoke test failed."
    echo "Check 'cf logs $app_name --recent' for more info."
    exit 1
  fi

  echo "$task_state"
}

delete_existing_service() {
  if cf service "$1"; then
    cf delete-service -f "$1"
    wait_for_deletion "$1"
  fi
}
