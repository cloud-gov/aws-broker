# Oracle 19c — service plans

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

Oracle is offered as a plan on the existing `aws-rds` service (not a separate
service), so it shares the RDS lifecycle, credential, and binding model.

## `oracle-19c-dev` (dev/test)

Defined in `catalog-template.yml` / `catalog-test.yml`:

| Field | Value |
|-------|-------|
| `dbType` | `oracle-ee` |
| `dbVersion` | `19.0.0.0.ru-2024-07.rur-2024-07.r1` (pinned) |
| `approvedMajorVersions` | `["19"]` |
| `licenseModel` | `bring-your-own-license` |
| `instanceClass` | `db.t3.medium` |
| `allocatedStorage` | 20 GB |
| `storage_type` | `gp3` |
| `encrypted` | `true` |
| `backup_retention_period` | 14 |
| `securityGroup` | `meta.aws_broker.oracle_security_group` (private) |
| `subnetGroup` | `meta.aws_broker.subnet_group` (private) |
| `free` | `false` |

> Provisioning requires `meta.aws_broker.oracle_security_group` to be defined in
> the environment (mirrors the postgres/mysql security-group pattern).

## Create parameters

The Oracle baseline is applied automatically (born hardened). Tenant-supplied
create/update parameters that would weaken the STIG baseline or enable
attack-surface options are rejected fail-closed
([#535](https://github.com/cloud-gov/aws-broker/issues/535)); broker-generated
identifiers are validated before the AWS call
([#524](https://github.com/cloud-gov/aws-broker/issues/524)).

## Production plans

Not defined yet. Production-ready plans (Multi-AZ, deletion protection, sized
instance classes) are added only after the live dev-RDS proof (WS15) confirms the
posture. See [limitations.md](limitations.md).
