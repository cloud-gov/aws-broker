#!/usr/bin/env bash
# scripts/lint-oracle-go.sh — golangci-lint gate for the Oracle-19c work (epic #519).
#
# aws-broker is a large inherited repo with ~53 pre-existing golangci-lint findings
# and no prior lint gate. We must NOT fail commits on inherited debt, but we DO need
# ALL our Oracle changes linted — including security-critical files this branch only
# *modified* (broker.go ENABLE_ORACLE gate #534, validate.go allowlist #535,
# credentials.go binding payload #528), not just files it created
# (adversarial-review H2).
#
# Approach: run golangci-lint on the package, then attribute each finding to the
# git commit that last touched that line (git blame). A finding is GATED only if its
# commit is on THIS branch (not an ancestor of the base ref). This is provenance-
# accurate — unlike --new-from-merge-base, which mis-attributes pre-existing findings
# whose line numbers merely shifted (verified: it flagged 11 findings all blamed to
# main). Inherited debt is excluded by authorship, not by a filename allowlist.
#
# Fail-closed on tool failure: if golangci-lint itself errors (crash/panic/missing
# binary/typecheck abort → exit >1) we FAIL, never emit a false green
# (adversarial-review H3). The previous `|| true` swallowed that.
set -euo pipefail

BASE_REF="${LINT_BASE_REF:-main}"
PKG="${LINT_PKG:-./services/rds/}"

# golangci-lint text lines look like:  services/rds/foo.go:123:45: msg (linter)
LINE_RE='^([^:]+\.go):([0-9]+):[0-9]+:'

set +e
out="$(golangci-lint run "$PKG" 2>&1)"
status=$?
set -e

if [[ "$status" -gt 1 ]]; then
	echo "golangci-lint failed to run (exit $status) — treating as a gate failure:" >&2
	printf '%s\n' "$out" >&2
	exit "$status"
fi

gated=""
while IFS= read -r line; do
	[[ "$line" =~ $LINE_RE ]] || continue
	file="${BASH_REMATCH[1]}"
	lno="${BASH_REMATCH[2]}"
	[[ -f "$file" ]] || continue
	# Commit that last touched this line in the WORKING TREE (not HEAD): blaming
	# HEAD mis-attributes uncommitted lines to whatever committed line they shifted
	# into, so a not-yet-committed finding would escape the gate. Blaming the
	# working tree marks uncommitted lines as all-zero ("Not Committed Yet").
	commit="$(git blame -L "${lno},${lno}" --porcelain -- "$file" 2>/dev/null | head -1 | awk '{print $1}')"
	[[ -n "$commit" ]] || continue
	# Uncommitted (working-tree) changes → always gated.
	if [[ "$commit" =~ ^0+$ ]]; then
		gated+="${line}"$'\n'
		continue
	fi
	# Pre-existing if the commit is an ancestor of the base ref; otherwise it's ours.
	if git merge-base --is-ancestor "$commit" "$BASE_REF" 2>/dev/null; then
		continue
	fi
	gated+="${line}"$'\n'
done <<<"$out"

if [[ -n "${gated// /}" ]]; then
	echo "golangci-lint findings introduced by this branch (must fix):" >&2
	printf '%s' "$gated" >&2
	exit 1
fi

echo "golangci-lint: no findings attributable to this branch's commits in ${PKG} (inherited debt excluded by git provenance; #519 follow-up widens the gate whole-repo)."
