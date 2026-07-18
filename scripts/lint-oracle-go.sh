#!/usr/bin/env bash
# scripts/lint-oracle-go.sh — golangci-lint gate scoped to the Oracle-19c files
# this branch authored (epic #519). aws-broker is a large inherited repo with
# ~53 pre-existing golangci-lint findings and no prior lint gate; this keeps the
# pre-commit signal honest (our Oracle code must be clean) without a red wall of
# inherited debt. A tracked follow-up burns down the debt and widens to ./... .
#
# Runs golangci-lint at the PACKAGE level (single-file mode fails typecheck
# because types like paramDetails span files), then fails only if a reported
# issue is in one of our authored Oracle files.
set -euo pipefail

# Files this branch authored for the Oracle work (100% new).
OWNED_RE='services/rds/(engine|engine_baselines|baselines|engine_test|baselines_test)\.go:'

out="$(golangci-lint run ./services/rds/ 2>&1 || true)"
hits="$(printf '%s\n' "$out" | grep -E "$OWNED_RE" || true)"

if [[ -n "$hits" ]]; then
	echo "golangci-lint findings in Oracle-19c authored files:" >&2
	printf '%s\n' "$hits" >&2
	exit 1
fi
echo "golangci-lint: Oracle-19c authored files clean (inherited debt not gated here; see #519 follow-up)."
