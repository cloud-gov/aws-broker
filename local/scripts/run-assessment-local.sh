#!/usr/bin/env bash
# run-assessment-local.sh — run the STIG assessment SQL against the local Oracle
# container and write a clearly-labeled report. Local harness only.
#
# ⚠️ DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE.
# Authoritative evidence comes from cg-oracle-database-19c-stig-overlay run against
# a real brokered GovCloud RDS Oracle instance.
#
# Runs sqlplus INSIDE the oracle-free container (docker exec), so no Oracle Instant
# Client / sqlplus is needed on the host.
set -euo pipefail

CONN="${1:?usage: run-assessment-local.sh <user/pass@//host:port/service>}"
CONTAINER="${ORACLE_CONTAINER:-cg-oracle-free}"
SQL_DIR="${SQL_DIR:-../../cg-oracle-database-19c-stig-overlay/hardening/sql}"
OUT_DIR="reports"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT_DIR}/assessment-${STAMP}.log"

mkdir -p "${OUT_DIR}"
{
	echo "=============================================================="
	echo " DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE"
	echo " local Oracle assessment @ ${STAMP}"
	echo "=============================================================="
} | tee "${OUT}"

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
	echo "oracle container '${CONTAINER}' is not running — run 'make oracle-up' first." | tee -a "${OUT}"
	exit 2
fi
if [ ! -d "${SQL_DIR}" ]; then
	echo "assessment SQL dir not found: ${SQL_DIR}" | tee -a "${OUT}"
	echo "(clone cg-oracle-database-19c-stig-overlay as a SIBLING of aws-broker, or set SQL_DIR=)" | tee -a "${OUT}"
	exit 2
fi

# Stage the SQL into the container in a fresh, world-readable dir (docker cp
# preserves host perms/owner, so reset them for the container's oracle user).
docker exec -u 0 "${CONTAINER}" rm -rf /tmp/hardening-sql >/dev/null 2>&1 || true
docker cp "${SQL_DIR}/." "${CONTAINER}:/tmp/hardening-sql" >/dev/null
docker exec -u 0 "${CONTAINER}" chmod -R a+rX /tmp/hardening-sql >/dev/null 2>&1 || true

# Run only assessment-first scripts (00_*, 01_*, *_assess.sql, 90_validate.sql).
shopt -s nullglob
for f in "${SQL_DIR}"/00_*.sql "${SQL_DIR}"/01_*.sql "${SQL_DIR}"/*_assess.sql "${SQL_DIR}"/90_validate.sql; do
	base="$(basename "${f}")"
	echo ">>> ${base}" | tee -a "${OUT}"
	docker exec -i "${CONTAINER}" bash -lc "cd /tmp/hardening-sql && sqlplus -S '${CONN}' @'${base}'" | tee -a "${OUT}"
done

echo "assessment written to ${OUT} (development signal only)"
