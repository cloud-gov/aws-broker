#!/usr/bin/env bash
# run-hardening-local.sh — apply the ALLOWED hardening SQL against local Oracle.
# Local harness only (WS10/11, #529).
#
# ⚠️ DEVELOPMENT SIGNAL ONLY (ADR-0005). Hardening is assessment-first + idempotent;
# destructive / PUBLIC-grant changes are detect-first and require an explicit
# allowlist (they are NOT applied automatically). RDS-incompatible commands skip
# with a reason. Develop as a NON-SYS user (see init/00_create_test_users.sql).
set -euo pipefail

CONN="${1:?usage: run-hardening-local.sh <user/pass@//host:port/service>}"
SQL_DIR="${SQL_DIR:-../../cg-oracle-database-19c-stig-overlay/hardening/sql}"
OUT_DIR="reports"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${OUT_DIR}/hardening-${STAMP}.log"

mkdir -p "${OUT_DIR}"
{
  echo "=============================================================="
  echo " DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE (ADR-0005)"
  echo " local Oracle hardening @ ${STAMP}"
  echo "=============================================================="
} | tee "${OUT}"

if [ ! -d "${SQL_DIR}" ]; then
  echo "hardening SQL dir not found: ${SQL_DIR}" | tee -a "${OUT}"
  echo "(the authoritative hardening SQL lives in the overlay repo, ADR-0002)" | tee -a "${OUT}"
  exit 2
fi

# Apply remediation scripts (10_*..50_*), skipping assessment-only + rollback.
shopt -s nullglob
for f in "${SQL_DIR}"/10_*.sql "${SQL_DIR}"/20_*.sql "${SQL_DIR}"/30_*.sql; do
  case "${f}" in
    *_assess.sql) continue ;;
  esac
  echo ">>> ${f}" | tee -a "${OUT}"
  sqlplus -S "${CONN}" @"${f}" | tee -a "${OUT}"
done

echo "hardening applied; re-run 'make assess' to see the before/after (dev signal only)"
