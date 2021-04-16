#!/bin/sh

set -e -x

# Copy application to output
cp -r aws-broker-app/* built

# Fetch remote stack state
aws s3 cp s3://$S3_TFSTATE_BUCKET/$BASE_STACK_NAME/terraform.tfstate stack.tfstate

TERRAFORM="${TERRAFORM_BIN:-terraform}"

# Append environment variables to manifest
cat << EOF >> built/manifest.yml
env:
  DB_URL: `${TERRAFORM} output -state=$STATE_FILE rds_internal_rds_host`
  DB_PORT: `${TERRAFORM} output -state=$STATE_FILE rds_internal_rds_port`
  ENABLE_FUNCTIONS: true
EOF

# Build secrets for merging into templates
cat << EOF > built/credentials.yml
meta:
  environment: $ENVIRONMENT
  aws_broker:
    subnet_group: `${TERRAFORM} output -state stack.tfstate rds_subnet_group`
    postgres_security_group: `${TERRAFORM} output -state=stack.tfstate rds_postgres_security_group`
    mysql_security_group: `${TERRAFORM} output -state=stack.tfstate rds_mysql_security_group`
    oracle_security_group: `${TERRAFORM} output -state=stack.tfstate rds_oracle_security_group`
    mssql_security_group: `${TERRAFORM} output -state=stack.tfstate rds_mssql_security_group`
  shared_mysql:
    name: $RDS_SHARED_MYSQL_NAME
    username: $RDS_SHARED_MYSQL_USERNAME
    password: $RDS_SHARED_MYSQL_PASSWORD
    url: `${TERRAFORM} output -state=$STATE_FILE rds_shared_mysql_rds_host`
    port: `${TERRAFORM} output -state=$STATE_FILE rds_shared_mysql_rds_port`
  shared_postgres:
    name: $RDS_SHARED_POSTGRES_NAME
    username: $RDS_SHARED_POSTGRES_USERNAME
    password: $RDS_SHARED_POSTGRES_PASSWORD
    url: `${TERRAFORM} output -state=$STATE_FILE rds_shared_postgres_rds_host`
    port: `${TERRAFORM} output -state=$STATE_FILE rds_shared_postgres_rds_port`
  redis: 
    subnet_group: `${TERRAFORM} output -state stack.tfstate elasticache_subnet_group`
    security_group: `${TERRAFORM} output -state stack.tfstate elasticache_redis_security_group`
  elasticsearch:
    subnet_id_az1: `${TERRAFORM} output -state stack.tfstate elasticsearch_subnet_az1`
    subnet_id_az2: `${TERRAFORM} output -state stack.tfstate elasticsearch_subnet_az2`
    security_group: `${TERRAFORM} output -state stack.tfstate elasticsearch_security_group`
EOF

# Merge secrets into templates
spruce merge aws-broker-app/secrets-template.yml built/credentials.yml > built/secrets.yml
spruce merge aws-broker-app/catalog-template.yml built/credentials.yml > built/catalog.yml
