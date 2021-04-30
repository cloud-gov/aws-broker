#!/bin/bash

set -e

export PGPASSWORD="${POSTGRES_PASSWORD}"

TERRAFORM="${TERRAFORM_BIN:-terraform}"

host=$(${TERRAFORM} output -raw -state="${STATE_FILE}" rds_shared_postgres_rds_host)
extensions=("hstore" "pg_trgm")

for extension in "${extensions[@]}"; do
  psql -h "${host}" -U "${POSTGRES_USERNAME}" -d template1 -c "create extension if not exists ${extension};"
done
