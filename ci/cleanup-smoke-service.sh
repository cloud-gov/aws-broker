#!/bin/bash
# cleanup-smoke-service.sh — best-effort teardown of leftover smoke-test service
# instances + apps for a given SERVICE_PLAN, run from a Concourse `ensure:` step so
# a mid-run failure of the smoke test cannot leak a live (and, for Oracle EE,
# expensive) RDS instance + its backups. Epic #519.
#
# The smoke runners use randomized instance names
# (rds-smoke-tests[-db-update]-$SERVICE_PLAN-$TEST_ID); this sweeps every service
# whose name contains the plan and force-deletes it plus any bound apps. Never
# fails the build (teardown is best-effort): the smoke task's own on_failure/
# on_success is what reports the actual result.
set -uo pipefail

# ci-utils.sh is provided at runtime by the Concourse `aws-broker-app` input; it is
# not resolvable at lint time. This is the same pattern the sibling ci/*.sh scripts
# use.
# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

login || true

: "${SERVICE_PLAN:?SERVICE_PLAN must be set}"

# Find service instances belonging to this plan's smoke tests and delete them.
# `cf services` output is best-effort parsed; guard everything so we never exit
# nonzero from the ensure step.
mapfile -t services < <(cf services 2>/dev/null | awk 'NR>3 {print $1}' | grep -E "rds-smoke-tests.*${SERVICE_PLAN}" || true)

for svc in "${services[@]}"; do
	[ -n "$svc" ] || continue
	echo "cleanup: deleting leftover smoke service ${svc} (and its bindings)"
	# Unbind/delete any apps still bound, then delete the service.
	mapfile -t apps < <(cf curl "/v3/service_instances?names=${svc}" 2>/dev/null | jq -r '.resources[0].guid // empty' | while read -r guid; do
		[ -n "$guid" ] && cf curl "/v3/service_credential_bindings?service_instance_guids=${guid}" 2>/dev/null | jq -r '.resources[].relationships.app.data.guid // empty'
	done)
	for app_guid in "${apps[@]}"; do
		[ -n "$app_guid" ] || continue
		app_name=$(cf curl "/v3/apps/${app_guid}" 2>/dev/null | jq -r '.name // empty')
		[ -n "$app_name" ] && cf delete -f "$app_name" || true
	done
	cf delete-service -f "$svc" || true
done

echo "cleanup: done (best-effort) for plan ${SERVICE_PLAN}"
exit 0
