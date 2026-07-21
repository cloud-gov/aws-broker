#!/usr/bin/env bash
# wait-for-oracle.sh — block until the local Oracle container is healthy.
# Local harness only; development signal only.
set -euo pipefail

CONTAINER="${1:-cg-oracle-free}"
TIMEOUT="${2:-300}"

echo "Waiting up to ${TIMEOUT}s for ${CONTAINER} to become healthy..."
elapsed=0
while true; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "${CONTAINER}" 2>/dev/null || echo "missing")"
  case "${status}" in
    healthy) echo "${CONTAINER} is healthy."; exit 0 ;;
    missing) echo "container ${CONTAINER} not found"; exit 1 ;;
  esac
  if [ "${elapsed}" -ge "${TIMEOUT}" ]; then
    echo "timed out waiting for ${CONTAINER} (last status: ${status})"; exit 1
  fi
  sleep 5; elapsed=$((elapsed + 5))
done
