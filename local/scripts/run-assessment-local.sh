#!/usr/bin/env bash
# run-assessment-local.sh — run the STIG assessment SQL against the local Oracle
# and write a clearly-labeled report. Local harness only (WS10/11, #529).
#
# ⚠️ DEVELOPMENT SIGNAL ONLY (ADR-0005) — NOT COMPLIANCE EVIDENCE.
# Authoritative evidence comes from cg-oracle-database-19c-stig-overlay run against
# a real brokered GovCloud RDS Oracle instance.
set -euo pipefail

CONN="${1:?usage: run-assessment-local.sh <user/pass@//host:port/service>}"
SQL_DIR="${SQL_DIR:-../../cg-oracle-database-19c-stig-overlay/hardening/sql}"
OUT_DIR="reports"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT_DIR}/assessment-${STAMP}.log"

mkdir -p "${OUT_DIR}"
{
  echo "=============================================================="
  echo " DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE (ADR-0005)"
  echo " local Oracle assessment @ ${STAMP}"
  echo "=============================================================="
} | tee "${OUT}"

if [ ! -d "${SQL_DIR}" ]; then
  echo "assessment SQL dir not found: ${SQL_DIR}" | tee -a "${OUT}"
  echo "(the authoritative hardening/assessment SQL lives in the overlay repo, ADR-0002)" | tee -a "${OUT}"
  exit 2
fi

# Run only assessment-first scripts (00_*, 01_*, *_assess.sql, 90_validate.sql).
shopt -s nullglob
for f in "${SQL_DIR}"/00_*.sql "${SQL_DIR}"/01_*.sql "${SQL_DIR}"/*_assess.sql "${SQL_DIR}"/90_validate.sql; do
  echo ">>> ${f}" | tee -a "${OUT}"
  sqlplus -S "${CONN}" @"${f}" | tee -a "${OUT}"
done

echo "assessment written to ${OUT} (development signal only)"
