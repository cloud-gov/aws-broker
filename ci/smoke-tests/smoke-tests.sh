#!/bin/bash

set -exuo pipefail

if [[ "$DB_TYPE" == "postgres" ]] ; then

  if [[ -v DB_VERSION ]]; then
    version=$(psql "$DATABASE_URL" -c "SHOW server_version")
    [[ "$version" == "$DB_VERSION" ]] || exit 1
  fi

  psql "$DATABASE_URL" -c "create table smoke (id integer, name text);"
  psql "$DATABASE_URL" -c "insert into smoke values (1, 'smoke');"
  psql "$DATABASE_URL" -c "drop table smoke;"

elif [[ "$DB_TYPE" = "mysql" ]] ; then

  MYSQL_HOST=$(echo "$VCAP_SERVICES" | jq -c -r '.["aws-rds"] | .[0].credentials.host')
  MYSQL_USER=$(echo "$VCAP_SERVICES" | jq -c -r '.["aws-rds"] | .[0].credentials.username')
  MYSQL_PASS=$(echo "$VCAP_SERVICES" | jq -c -r '.["aws-rds"] | .[0].credentials.password')
  MYSQL_DB=$(echo "$VCAP_SERVICES" | jq -c -r '.["aws-rds"] | .[0].credentials.db_name')

  if [[ -v DB_VERSION ]]; then
    version=$(mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" "-p$MYSQL_PASS" -e "SELECT VERSION();" "$MYSQL_DB")
    [[ "$version" == "$DB_VERSION" ]] || exit 1
  fi

  mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" "-p$MYSQL_PASS" -e "insert into smoke values (1, 'smoke');" "$MYSQL_DB"
  mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" "-p$MYSQL_PASS" -e "create table smoke (id integer, name text);" "$MYSQL_DB"
  mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" "-p$MYSQL_PASS" -e "drop table smoke;" "$MYSQL_DB"
  mysql -h "$MYSQL_HOST" -u "$MYSQL_USER" "-p$MYSQL_PASS" -e "create function hello(id INT) returns CHAR(50) return 'foobar';" "$MYSQL_DB"

else
  echo "\$DB_TYPE must be one of: postgres mysql"
  exit 1
fi

python -m SimpleHTTPServer "$PORT"
