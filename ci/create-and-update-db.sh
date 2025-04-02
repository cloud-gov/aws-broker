#!/bin/sh
set -e
set -u

TERRAFORM="${TERRAFORM_BIN:-terraform}"

# Check environment variables
db_address=$(${TERRAFORM} output -raw -state="${STATE_FILE_PATH}" "${TERRAFORM_DB_HOST_FIELD}")
db_user="${DB_USERNAME}"
db_pass="${DB_PASSWORD}"

export PGPASSWORD="${db_pass:?}"

# See: https://github.com/koalaman/shellcheck/wiki/SC2086#exceptions
psql_adm() { psql -h "${db_address}" -U "${db_user}" "$@"; }

# contains(string, substring)
# See: http://stackoverflow.com/questions/2829613/how-do-you-tell-if-a-string-contains-another-string-in-unix-shell-scripting
# Returns 0 if the specified string contains the specified substring,
# otherwise returns 1.
contains() {
    string="$1"
    substring="$2"
    if test "${string#*"$substring"}" != "$string"
    then
        return 0    # $substring is in $string
    else
        return 1    # $substring is not in $string
    fi
}

# Make sure that we create a default database for the db_user
# PG will assume a database named like the db_user if one is not specified.
if ! contains "$DATABASES" "$db_user"; then
  DATABASES="$db_user $DATABASES"
fi



for db in ${DATABASES}; do

  # Create database
  psql_adm -d postgres -l | awk '{print $1}' | grep -q "${db}" || \
    psql_adm -d postgres -c "CREATE DATABASE ${db} OWNER ${db_user}"
  # Enable extensions
  for ext in citext uuid-ossp pgcrypto pg_stat_statements; do
    psql_adm -d "${db}" -c "CREATE EXTENSION IF NOT EXISTS \"${ext}\""
  done

  # Remove default privileges
  psql_adm -d "${db}" -c "REVOKE ALL ON SCHEMA public FROM PUBLIC"
  

done
