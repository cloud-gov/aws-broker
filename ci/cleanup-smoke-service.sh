#!/bin/bash
# cleanup-smoke-service.sh — best-effort teardown of leftover smoke-test service
# instances (and their bound apps) for a given SERVICE_PLAN, run from a Concourse
# `ensure:` step so a mid-run smoke-test failure cannot leak a live billed
# instance + its backups.
#
# Engine-agnostic: works for any smoke-tested service (RDS / Elasticache /
# Elasticsearch). Smoke runners name instances <prefix>-<SERVICE_PLAN>-<id>;
# this deletes every service whose name contains the plan, which cascades to its
# bindings. Never fails the build — teardown is best-effort; the smoke task's own
# on_success/on_failure reports the real result.
set -uo pipefail

# ci-utils.sh is provided at runtime by the Concourse `aws-broker-app` input.
# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

login || true

: "${SERVICE_PLAN:?SERVICE_PLAN must be set}"

# `cf delete-service -f` cascades to bindings/keys, so no per-app teardown is
# needed. Match smoke-test instances for this plan and delete each.
cf services 2>/dev/null | awk 'NR>3 {print $1}' | grep -E "smoke-tests.*${SERVICE_PLAN}" |
	while read -r svc; do
		[ -n "$svc" ] || continue
		echo "cleanup: deleting leftover smoke service ${svc}"
		cf delete-service -f "$svc" || true
	done

echo "cleanup: done (best-effort) for plan ${SERVICE_PLAN}"
exit 0
