#!/usr/bin/env bash
# moto-smoke.sh — exercise the broker's Oracle RDS control-plane calls against the
# moto mock (layer 2). Confirms the AWS API ACCEPTS the shape the broker builds:
# create-db-instance (oracle-se2 19c, encrypted, License Included, private) +
# create-db-parameter-group (oracle-se2-19) + create-option-group (oracle-se2 19).
#
# DEVELOPMENT SIGNAL ONLY: moto mocks the control plane — it does NOT run
# an Oracle engine, apply parameters, or do a real TLS handshake. This proves the
# request shape, not RDS behavior. Requires: `make moto-up` first, and the aws CLI.
set -euo pipefail

# aws flags for the moto endpoint (array so it word-splits cleanly).
MOTO_ENDPOINT=(--endpoint-url http://localhost:5000 --region us-gov-west-1)
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
INSTANCE_ID="cg-oracle-smoke-$$"
PARAM_GROUP="cg-aws-broker-oracle-smoke-$$"
OPTION_GROUP="cg-aws-broker-oracle-smoke-og-$$"

if ! command -v aws >/dev/null 2>&1; then
	echo "aws CLI not found — 'brew install awscli' (layer-2 smoke only)." >&2
	exit 2
fi
MOTO_TIMEOUT="${MOTO_TIMEOUT:-10}"
elapsed=0
until curl -sf http://localhost:5000/moto-api/ >/dev/null 2>&1; do
	if [ "$elapsed" -ge "$MOTO_TIMEOUT" ]; then
		echo "moto is not up on :5000 after ${MOTO_TIMEOUT}s — run 'make moto-up' first." >&2
		exit 2
	fi
	sleep 1
	elapsed=$((elapsed + 1))
done

cleanup() {
	aws "${MOTO_ENDPOINT[@]}" rds delete-db-instance --db-instance-identifier "$INSTANCE_ID" --skip-final-snapshot >/dev/null 2>&1 || true
	aws "${MOTO_ENDPOINT[@]}" rds delete-db-parameter-group --db-parameter-group-name "$PARAM_GROUP" >/dev/null 2>&1 || true
	aws "${MOTO_ENDPOINT[@]}" rds delete-option-group --option-group-name "$OPTION_GROUP" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== create-db-instance (oracle-se2, License Included, encrypted, private) =="
aws "${MOTO_ENDPOINT[@]}" rds create-db-instance \
	--db-instance-identifier "$INSTANCE_ID" \
	--db-instance-class db.t3.medium \
	--engine oracle-se2 \
	--engine-version 19.0.0.0.ru-2024-07.rur-2024-07.r1 \
	--license-model license-included \
	--allocated-storage 20 \
	--master-username APPUSER01 \
	--master-user-password "fakePw12345" \
	--db-name ORCL \
	--storage-encrypted \
	--no-publicly-accessible \
	--backup-retention-period 14 \
	--query 'DBInstance.[DBInstanceIdentifier,Engine,EngineVersion,StorageEncrypted,LicenseModel,PubliclyAccessible]' \
	--output text

echo "== create-db-parameter-group (oracle-se2-19) =="
aws "${MOTO_ENDPOINT[@]}" rds create-db-parameter-group \
	--db-parameter-group-name "$PARAM_GROUP" \
	--db-parameter-group-family oracle-se2-19 \
	--description "cg smoke" \
	--query 'DBParameterGroup.[DBParameterGroupName,DBParameterGroupFamily]' --output text

echo "== create-option-group (oracle-se2 19) =="
aws "${MOTO_ENDPOINT[@]}" rds create-option-group \
	--option-group-name "$OPTION_GROUP" \
	--engine-name oracle-se2 \
	--major-engine-version 19 \
	--option-group-description "cg smoke" \
	--query 'OptionGroup.[OptionGroupName,EngineName,MajorEngineVersion]' --output text

echo ""
echo "moto smoke OK — the AWS API accepted the broker's Oracle create/param/option shapes."
echo "(development signal only: moto did not run Oracle, apply params, or do TLS.)"
