#!/bin/bash
# cleanup-smoke-service.sh — best-effort teardown of leftover smoke-test SERVICE
# INSTANCES for a given SERVICE_PLAN, run from a Concourse `ensure:` step so a
# mid-run smoke-test failure cannot leak a live billed instance + its backups.
#
# Engine-agnostic: works for any smoke-tested service (RDS / Elasticache /
# Elasticsearch). The smoke runners name their service INSTANCES with one of a
# small set of prefixes, all ending in the plan name (optionally with a trailing
# -<TEST_ID>). We match those instances for THIS plan exactly and delete each;
# `cf delete-service -f` also removes the instance's service BINDINGS/KEYS. It does
# NOT delete the separate pushed test APP (that is a distinct CF object) — the
# billed resource is the service instance, which is what we reap here; a leftover
# stopped app is cheap and the runners delete their own apps on the happy path.
# Never fails the build — teardown is best-effort; the smoke task's own
# on_success/on_failure reports the real result. A login failure is surfaced as a
# loud, greppable WARNING (not a silent green no-op) so operators can alert on a
# reaper that never actually reaped.
set -uo pipefail

# ci-utils.sh is provided at runtime by the Concourse `aws-broker-app` input.
# shellcheck disable=SC1091
. aws-broker-app/ci/ci-utils.sh

: "${SERVICE_PLAN:?SERVICE_PLAN must be set}"

# login() ends with `set -x`; we do not want the rest of this reaper traced.
if ! login; then
	echo "cleanup: WARN cf login failed — leftover smoke instances for plan '${SERVICE_PLAN}' were NOT reaped" >&2
	set +x
	exit 0
fi
set +x

# Service-INSTANCE name shapes the smoke runners create (see ci/run-smoke-test*.sh):
#   rds-smoke-tests-<plan>                         (run-smoke-tests.sh)
#   rds-smoke-tests-db-update-<plan>-<id>          (run-smoke-tests-db-updates.sh)
#   rds-smoke-tests-db-version-<plan>-<id>         (run-smoke-tests-db-version.sh)
#   rds-smoke-tests-db-rotate-creds-<plan>         (run-smoke-test-rotate-creds.sh)
#   redis-smoke-tests-update-<plan>-<id>           (run-smoke-tests-update-redis.sh)
#   smoke-test-<plan>-service                      (run-smoke-test-task.sh)
#   smoke-test-<plan>-unbound-service              (run-smoke-test-unbound.sh)
#   smoke-test-adv-<plan>-service                  (run-smoke-test-es-advanced-options.sh)
#   smoke-test-esver-<plan>-service                (run-smoke-test-es-version.sh)
# where <id> is $RANDOM (digits).
#
# Match rule (anchored, token-aware so we never reap a DIFFERENT plan's instance):
#   ^<prefix>(-<segment>)*-?<plan><suffix>$
#   prefix : `smoke-test-` (singular) OR `<word>-smoke-tests-` (plural)
#   segment: zero or more WHOLE hyphen-delimited segments (adv/esver/db-update/...)
#   plan   : SERVICE_PLAN as a LITERAL (regex metachars escaped)
#   suffix : end-of-name | -service | -unbound-service | -<digits>
# This deliberately does NOT match e.g. `...-redis-dev-large` when the plan is
# `redis-dev` (the `-large` makes it a different token, so the anchored suffix
# fails) — preventing collateral deletion of a concurrent related-plan run.
# shellcheck disable=SC2016  # the single-quoted sed script is intentionally literal
esc_plan=$(printf '%s' "$SERVICE_PLAN" | sed -e 's/[.[\*^$()+?{|]/\\&/g')
name_re="^(smoke-test-|[a-z0-9]+-smoke-tests-)([a-z0-9]+-)*${esc_plan}(-service|-unbound-service|-[0-9]+)?$"

matched=0
deleted=0
while read -r svc; do
	[ -n "$svc" ] || continue
	matched=$((matched + 1))
	echo "cleanup: deleting leftover smoke service ${svc}"
	if cf delete-service -f "$svc"; then
		deleted=$((deleted + 1))
	else
		echo "cleanup: WARN failed to delete ${svc} (continuing)" >&2
	fi
done < <(cf services 2>/dev/null | awk 'NR>3 {print $1}' | grep -E "$name_re" || true)

if [ "$matched" -eq 0 ]; then
	echo "cleanup: no leftover smoke instances found for plan '${SERVICE_PLAN}'"
else
	echo "cleanup: done (best-effort) — matched ${matched}, deleted ${deleted} for plan '${SERVICE_PLAN}'"
fi
exit 0
