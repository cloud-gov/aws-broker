#!/bin/bash

set -euxo pipefail

# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

UPGRADE_TIMEOUT_SECONDS=${UPGRADE_TIMEOUT_SECONDS:-5400}
INVALID_VERSION=${INVALID_VERSION:-Elasticsearch_6.8}
VALID_VERSION=${VALID_VERSION:-OpenSearch_2.11}
# Expected data-node counts for the plans under test. In the catalog every
# multi-node non-HA plan runs 2 data nodes and every -ha plan runs 4.
NEW_SERVICE_PLAN_DATA_NODES=${NEW_SERVICE_PLAN_DATA_NODES:-2}
TEST_REJECTIONS=${TEST_REJECTIONS:-""}

# Log in to CF
login

TEST_ID=$(get_test_id)
APP_NAME="search-smoke-tests-update-$SERVICE_PLAN-$TEST_ID-app"
SERVICE_NAME="search-smoke-tests-update-$SERVICE_PLAN-$TEST_ID-service"
TASK_DIRECTORY="aws-broker-app/ci/smoke-tests/aws-elasticsearch/"

# Re-bind to pick up credentials for the resized domain, then confirm the cluster
# still indexes and returns documents. Takes an optional expected data-node count,
# which is asserted against the live cluster health.
rebind_and_verify() {
  local expected_data_nodes=${1:-}
  local task_command="python run.py -s $SERVICE_NAME -r $REGION"
  local app_guid

  if [ -n "$expected_data_nodes" ]; then
    task_command="$task_command --expected-data-nodes $expected_data_nodes"
  fi

  cf unbind-service "$APP_NAME" "$SERVICE_NAME"
  wait_for_service_bindable "$APP_NAME" "$SERVICE_NAME"
  cf restage "$APP_NAME"
  cf run-task "$APP_NAME" --command "$task_command"
  app_guid=$(cf curl "/v3/apps?names=$APP_NAME" | jq -r ".resources[0].guid")
  get_task_state "$app_guid"
}

# Clean up any leftovers from a previous run
cf delete -f "$APP_NAME"
delete_existing_service "$SERVICE_NAME"

# Push the test app so we can prove the cluster still serves traffic, and still
# holds its data, after the resize.
pushd "$TASK_DIRECTORY"
cf push "$APP_NAME" -f manifest.yml
popd

# Create the instance on the starting plan.
cf create-service aws-elasticsearch "$SERVICE_PLAN" "$SERVICE_NAME" -b "$BROKER_NAME"
wait_for_service_instance_success "$SERVICE_NAME"

# Bind and index a document, establishing the "before" state.
wait_for_service_bindable "$APP_NAME" "$SERVICE_NAME"
cf restage "$APP_NAME"
cf run-task "$APP_NAME" --command "python run.py -s $SERVICE_NAME -r $REGION"
app_guid=$(cf curl "/v3/apps?names=$APP_NAME" | jq -r ".resources[0].guid")
get_task_state "$app_guid"

if [ -n "$TEST_REJECTIONS" ]; then
  #
  # Rejected changes. Each of these must fail synchronously: the broker returns an
  # HTTP 400 before calling AWS, so no asynchronous job is created.
  #

  # Dropping to a single data node removes data nodes, which the broker refuses: a
  # one-node plan is provisioned on a single subnet with zone awareness off, and
  # shrinking the cluster would discard the shards those nodes hold.
  expect_update_service_rejected "$SERVICE_NAME" \
    "cannot reduce the number of data nodes" \
    -p "$SINGLE_NODE_PLAN"
  assert_service_plan "$SERVICE_NAME" "$SERVICE_PLAN"

  # An engine version that AWS does not offer as an upgrade target from the domain's
  # current version. This one does reach AWS (GetCompatibleVersions).
  expect_update_service_rejected "$SERVICE_NAME" \
    "is not a valid upgrade target" \
    -c '{"elasticsearchVersion": "'"$INVALID_VERSION"'"}'

  # A plan change and a version upgrade are separate AWS operations and cannot be
  # combined in a single update.
  expect_update_service_rejected "$SERVICE_NAME" \
    "plan change cannot be combined with an engine version upgrade" \
    -p "$NEW_SERVICE_PLAN" -c '{"elasticsearchVersion": "'"$VALID_VERSION"'"}'
  assert_service_plan "$SERVICE_NAME" "$SERVICE_PLAN"
fi

if [ -n "$NEW_SERVICE_PLAN" ]; then
  #
  # The permitted upgrade. This is an AWS blue/green deployment and is slow, so it
  # gets a bounded wait rather than the unbounded poll used elsewhere.
  #
  cf update-service "$SERVICE_NAME" -p "$NEW_SERVICE_PLAN"
  wait_for_service_instance_success_with_timeout "$SERVICE_NAME" "$UPGRADE_TIMEOUT_SECONDS"
  assert_service_plan "$SERVICE_NAME" "$NEW_SERVICE_PLAN"
  rebind_and_verify "$NEW_SERVICE_PLAN_DATA_NODES"
fi

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
wait_for_deletion "$SERVICE_NAME"
