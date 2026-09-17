#!/usr/bin/env bash
# run-hardening-local.sh — apply the ALLOWED hardening SQL against local Oracle.
# Local harness only.
#
# ⚠️ DEVELOPMENT SIGNAL ONLY. Hardening is assessment-first + idempotent;
# destructive / PUBLIC-grant changes are detect-first and require an explicit
# allowlist (they are NOT applied automatically). RDS-incompatible commands skip
# with a reason. Develop as a NON-SYS user (see init/00_create_test_users.sql).
#
# Runs sqlplus INSIDE the oracle-free container (docker exec), so no Oracle Instant
# Client / sqlplus is needed on the host.
set -euo pipefail

CONN="${1:?usage: run-hardening-local.sh <user/pass@//host:port/service>}"
CONTAINER="${ORACLE_CONTAINER:-cg-oracle-free}"
SQL_DIR="${SQL_DIR:-../../cg-oracle-database-19c-stig-overlay/hardening/sql}"
OUT_DIR="reports"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT_DIR}/hardening-${STAMP}.log"

mkdir -p "${OUT_DIR}"
{
	echo "=============================================================="
	echo " DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE"
	echo " local Oracle hardening @ ${STAMP}"
	echo "=============================================================="
} | tee "${OUT}"

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
	echo "oracle container '${CONTAINER}' is not running — run 'make oracle-up' first." | tee -a "${OUT}"
	exit 2
fi
if [ ! -d "${SQL_DIR}" ]; then
	echo "hardening SQL dir not found: ${SQL_DIR}" | tee -a "${OUT}"
	echo "(clone cg-oracle-database-19c-stig-overlay as a SIBLING of aws-broker, or set SQL_DIR=)" | tee -a "${OUT}"
	exit 2
fi

# Stage the SQL into the container in a fresh, world-readable dir (docker cp
# preserves host perms/owner, so reset them for the container's oracle user).
docker exec -u 0 "${CONTAINER}" rm -rf /tmp/hardening-sql >/dev/null 2>&1 || true
docker cp "${SQL_DIR}/." "${CONTAINER}:/tmp/hardening-sql" >/dev/null
docker exec -u 0 "${CONTAINER}" chmod -R a+rX /tmp/hardening-sql >/dev/null 2>&1 || true

# Apply remediation scripts (10_*..30_*), skipping assessment-only + rollback.
shopt -s nullglob
for f in "${SQL_DIR}"/10_*.sql "${SQL_DIR}"/20_*.sql "${SQL_DIR}"/30_*.sql; do
	case "${f}" in
	*_assess.sql) continue ;;
	esac
	base="$(basename "${f}")"
	echo ">>> ${base}" | tee -a "${OUT}"
	docker exec -i "${CONTAINER}" bash -lc "cd /tmp/hardening-sql && sqlplus -S '${CONN}' @'${base}'" | tee -a "${OUT}"
done

echo "hardening applied; re-run 'make assess' to see the before/after (dev signal only)"
