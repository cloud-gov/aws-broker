#!/bin/bash

set -euxo pipefail

# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

UPGRADE_TIMEOUT_SECONDS=${UPGRADE_TIMEOUT_SECONDS:-5400}
INVALID_VERSION=${INVALID_VERSION:-Elasticsearch_6.8}
VALID_VERSION=${VALID_VERSION:-OpenSearch_2.11}

# Log in to CF
login

TEST_ID="$RANDOM"
APP_NAME="search-smoke-tests-update-$SERVICE_PLAN-$TEST_ID-app"
SERVICE_NAME="search-smoke-tests-update-$SERVICE_PLAN-$TEST_ID-service"
TASK_DIRECTORY="aws-broker-app/ci/smoke-tests/aws-elasticsearch/"

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

#
# Rejected changes. Each of these must fail synchronously: the broker returns an
# HTTP 400 before calling AWS, so no asynchronous job is created.
#

# Crossing the HA tier changes zone awareness / subnet topology, which AWS cannot
# do in place on an existing domain.
expect_update_service_rejected "$SERVICE_NAME" \
  "cannot change between highly-available and non-highly-available plans" \
  -p "$HA_PLAN"
assert_service_plan "$SERVICE_NAME" "$SERVICE_PLAN"

# Dropping to a single data node is also a topology change: a one-node plan is
# provisioned on a single subnet with zone awareness off. Without this guard the
# broker sends AWS a zone-count of two for a domain that has one subnet, and AWS
# fails the update with "You must specify exactly two subnets because you've set
# zone count to two."
expect_update_service_rejected "$SERVICE_NAME" \
  "cannot change between single-node and multi-node plans" \
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

#
# The permitted upgrade. This is an AWS blue/green deployment and is slow, so it
# gets a bounded wait rather than the unbounded poll used elsewhere.
#
cf update-service "$SERVICE_NAME" -p "$NEW_SERVICE_PLAN"
wait_for_service_instance_success_with_timeout "$SERVICE_NAME" "$UPGRADE_TIMEOUT_SECONDS"
assert_service_plan "$SERVICE_NAME" "$NEW_SERVICE_PLAN"

# Re-bind to pick up credentials for the resized domain, then confirm the cluster
# still indexes and returns documents.
cf unbind-service "$APP_NAME" "$SERVICE_NAME"
wait_for_service_bindable "$APP_NAME" "$SERVICE_NAME"
cf restage "$APP_NAME"
cf run-task "$APP_NAME" --command "python run.py -s $SERVICE_NAME -r $REGION"
app_guid=$(cf curl "/v3/apps?names=$APP_NAME" | jq -r ".resources[0].guid")
get_task_state "$app_guid"

# Now that the instance is on the larger plan, going back down must be rejected.
expect_update_service_rejected "$SERVICE_NAME" \
  "downgrading to a smaller plan is not supported" \
  -p "$SERVICE_PLAN"
assert_service_plan "$SERVICE_NAME" "$NEW_SERVICE_PLAN"

# Clean up app and service
cf delete -f "$APP_NAME"
cf delete-service -f "$SERVICE_NAME"
wait_for_deletion "$SERVICE_NAME"
