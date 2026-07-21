# Oracle 19c broker validation contract

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS12/13
> ([#530](https://github.com/cloud-gov/aws-broker/issues/530)). Interface between
> the broker (provisions/configures) and the overlay (validates) — [ADR-0002](../decisions/ADR-0002-keep-stig-validation-in-overlay-repo.md).

The **validation contract** is a JSON document the broker (or an operator, from
broker metadata) renders to describe a provisioned RDS Oracle 19c instance. The
`cg-oracle-database-19c-stig-overlay` consumes it to (a) know how to connect and
(b) layer each STIG control correctly for a *managed RDS* target.

Schema: [`validation-contract.schema.json`](validation-contract.schema.json).

## Hard rule: no secrets

The contract MUST NOT contain plaintext passwords, AWS secret keys, admin
credentials, or customer data. Credentials are referenced by **opaque ref**
(`assessment_credential_ref`, `app_credential_ref`) that the operator resolves
out-of-band (binding GUID / secret-store path). The renderer fails closed if a
forbidden key (`password`, `master_password`, `aws_secret_access_key`, …) is
present.

## Example

```json
{
  "contract_version": "1.0",
  "service_instance_id": "b1f2...",
  "broker": "aws-broker",
  "plan": "oracle-se2-license-included-dev",
  "environment": "aws-broker-rds-dev",
  "aws_partition": "aws-us-gov",
  "aws_region": "us-gov-west-1",
  "engine": "oracle-se2",
  "engine_version": "19.0.0.0.ru-2024-07.rur-2024-07.r1",
  "endpoint": "cg-oracle-dev.abc123.us-gov-west-1.rds.amazonaws.com",
  "port": 2484,
  "protocol": "tcps",
  "service_name": "ORCL",
  "db_name": "ORCL",
  "parameter_group": "cg-aws-broker-...-version-19",
  "option_group": "cg-aws-broker-...-option-19",
  "ssl_enabled": true,
  "ssl_version": "1.2",
  "kms_key_id": "arn:aws-us-gov:kms:us-gov-west-1:...:key/...",
  "storage_encrypted": true,
  "publicly_accessible": false,
  "multi_az": false,
  "backup_retention_period": 14,
  "deletion_protection": true,
  "cloudwatch_log_exports": ["alert", "audit", "listener"],
  "assessment_credential_ref": "binding:guid:...",
  "app_credential_ref": "binding:guid:...",
  "tags": { "environment": "dev" }
}
```

## How the overlay consumes it

1. Resolve `assessment_credential_ref` out-of-band → DB user/password.
2. Render the InSpec `--input-file` (`user`, `password`, `host` from `endpoint` or
   the tunnel, `port`, `service` from `service_name`, `sqlplus_bin`) plus the
   org-policy allow-list inputs. `port` is the SSL/TCPS listener (`2484`) when
   `ssl_enabled` is true; `protocol`/`ssl_version` let the overlay assert the
   encryption-in-transit posture (SC-8 / SC-13). **Caveat:** TLS-only reachability
   depends on the platform security group ([#541](https://github.com/cloud-gov/aws-broker/issues/541));
   `ssl_enabled` reflects the configured posture, not a live-enforced TLS-only state.
3. Apply [`control-layers.yml`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/blob/main/control-layers.yml)
   so controls that are `aws_rds_parameter_group` / `aws_inherited` /
   `not_applicable_rds` report correctly instead of failing on a managed RDS.
4. `environment` tells the overlay whether this is `local-dev` (development signal
   only, ADR-0005) or real RDS evidence.

## Rendering

A renderer that produces this contract from broker instance metadata is a
follow-up (it needs live AWS `DescribeDBInstances` fields); the schema + this doc
define the shape now so the overlay side (control-layers map) can be built in
parallel.
