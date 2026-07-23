#!/usr/bin/env bash
# scripts/lint-oracle-go.sh — golangci-lint gate for the Oracle-19c work.
#
# aws-broker is a large inherited repo with pre-existing golangci-lint findings
# and no prior lint gate. We must NOT fail commits on inherited debt, but we DO need
# ALL our Oracle changes linted — including security-critical files this branch only
# *modified* (broker.go ENABLE_ORACLE gate, validate.go allowlist, credentials.go
# binding payload), not just files it created.
#
# Approach: run golangci-lint on the package, then attribute each finding to the
# git commit that last touched that line (git blame). A finding is GATED unless we
# can prove it is pre-existing (its commit is an ancestor of the base ref). This is
# provenance-accurate — unlike --new-from-merge-base, which mis-attributes
# pre-existing findings whose line numbers merely shifted.
#
# FAIL-CLOSED throughout: this gate exists to give honest signal on OUR changes, so
# any INABILITY to attribute a finding (missing file, empty/failed blame, out-of-
# range line) results in the finding being GATED (reported), never silently dropped
# — the opposite of failing open. golangci-lint's own tool errors (exit >1) also
# fail the gate. Only a finding we can POSITIVELY prove is an ancestor of the base
# ref is excluded.
set -uo pipefail

BASE_REF="${LINT_BASE_REF:-origin/main}"
PKG="${LINT_PKG:-./services/rds/}"

# The base ref must exist and be resolvable, or "ancestor of base" is meaningless
# and we would over-gate silently. Prefer origin/main; fall back to main; else fail
# loudly (do not silently treat every commit as new).
if ! git rev-parse --verify --quiet "${BASE_REF}^{commit}" >/dev/null; then
	if git rev-parse --verify --quiet "main^{commit}" >/dev/null; then
		BASE_REF="main"
	else
		echo "lint-oracle-go: base ref '${BASE_REF}' (and fallback 'main') not found; " \
			"fetch it or set LINT_BASE_REF. Refusing to run (would mis-gate)." >&2
		exit 2
	fi
fi

# golangci-lint default (colored-line-number) output looks like:
#   services/rds/foo.go:123:45: msg (linter)
# Allow optional leading whitespace; the path token has no spaces or ':'.
LINE_RE='^[[:space:]]*([^[:space:]:]+\.go):([0-9]+):[0-9]+:'

out="$(golangci-lint run "$PKG" 2>&1)"
status=$?

if [[ "$status" -gt 1 ]]; then
	echo "golangci-lint failed to run (exit $status) — treating as a gate failure:" >&2
	printf '%s\n' "$out" >&2
	exit "$status"
fi

# is_ancestor: 0 (true) only if $1 is provably an ancestor of $BASE_REF.
# git merge-base --is-ancestor returns 0=ancestor, 1=not-ancestor, 128=error; we
# treat anything other than a clean 0 as "not provably pre-existing" → caller gates.
is_ancestor() {
	git merge-base --is-ancestor "$1" "$BASE_REF" >/dev/null 2>&1
}

gated=""
gate() { gated+="$1"$'\n'; }

while IFS= read -r line; do
	[[ "$line" =~ $LINE_RE ]] || continue
	file="${BASH_REMATCH[1]}"
	lno="${BASH_REMATCH[2]}"

	# Cannot attribute → GATE (fail closed), never drop.
	if [[ ! -f "$file" ]]; then
		gate "$line  [gated: file not found for blame]"
		continue
	fi

	# Blame the WORKING TREE (not HEAD) so uncommitted lines are marked all-zero
	# ("Not Committed Yet") instead of being mis-attributed to a shifted commit.
	# A blame failure (e.g. out-of-range line) must NOT abort the whole gate
	# (set -e is off) and must NOT drop the finding → gate it.
	if ! blame="$(git blame -L "${lno},${lno}" --porcelain -- "$file" 2>/dev/null)"; then
		gate "$line  [gated: git blame failed]"
		continue
	fi
	commit="$(printf '%s\n' "$blame" | head -1 | awk '{print $1}')"
	if [[ -z "$commit" ]]; then
		gate "$line  [gated: empty blame]"
		continue
	fi

	# Uncommitted (working-tree) changes → always gated.
	if [[ "$commit" =~ ^0+$ ]]; then
		gate "$line"
		continue
	fi

	# Excluded ONLY if provably pre-existing (ancestor of the base ref).
	if is_ancestor "$commit"; then
		continue
	fi
	gate "$line"
done <<<"$out"

if [[ -n "${gated//[[:space:]]/}" ]]; then
	echo "golangci-lint findings introduced by this branch (must fix):" >&2
	printf '%s' "$gated" >&2
	exit 1
fi

echo "golangci-lint: no findings attributable to this branch's commits in ${PKG} (pre-existing debt excluded by git provenance against ${BASE_REF})."
