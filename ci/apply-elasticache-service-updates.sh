#!/bin/bash

set -euxo pipefail

# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

TASK_APP="aws-broker-tasks"

DRY_RUN="${DRY_RUN:-false}"

TASK_COMMAND="tasks -action apply-elasticache-service-updates -service elasticache"

if [ "$DRY_RUN" = "true" ]; then
  TASK_COMMAND="$TASK_COMMAND -dry-run"
fi

if [ -n "${BATCH_SIZE:-}" ]; then
  TASK_COMMAND="$TASK_COMMAND -batch-size $BATCH_SIZE"
fi

if [ -n "${STAGGER_DELAY:-}" ]; then
  TASK_COMMAND="$TASK_COMMAND -stagger-delay $STAGGER_DELAY"
fi

# Log into CF
login

# Run the task against the already-deployed aws-broker-tasks app.
cf run-task "$TASK_APP" --command "$TASK_COMMAND"

# Get finished task state with app guid
task_app_guid=$(cf curl "/v3/apps?names=$TASK_APP" | jq -r ".resources[0].guid")

get_task_state "$task_app_guid"
