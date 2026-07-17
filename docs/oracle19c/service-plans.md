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

> **Feature-gated.** Oracle provisioning is disabled unless the broker environment
> sets `ENABLE_ORACLE` (`EnableOracleFeature`). This is a fail-closed safeguard for
> the master-credential-reuse limitation ([#534](https://github.com/cloud-gov/aws-broker/issues/534)):
> the Oracle RDS master user holds DBA and is currently returned to every binding.
> `cf create-service … oracle-19c-dev …` returns an error pointing to #534 when the
> flag is unset.

## Create parameters

The Oracle baseline is applied automatically (born hardened). Because Oracle is a
**self-service** offering (app developers, not operators, run `cf create-service`),
the broker enforces an **allowlist** of customer-suppliable `-c` parameters and
rejects the rest fail-closed (#535) — so a customer cannot silently weaken the STIG
baseline or pass a MySQL/Postgres-only knob that would be a no-op.

**Supported for Oracle** (same shape customers already know from psql/mysql):

| Parameter | Notes |
|-----------|-------|
| `storage` | GB available to the instance |
| `backup_retention_period` | 14–35 days |
| `storage_type` | `gp3` |
| `rotate_credentials` | `true` to rotate the master password (then unbind/bind/restage) |
| `enable_cloudwatch_log_groups_exports` | subset of `["alert","audit","listener","trace","oemagent"]` (defaults to alert/audit/listener) |

**Rejected for Oracle** (fail-closed): `publicly_accessible` (private-only),
`enable_functions` / `binary_log_format` (MySQL-only), `enable_pg_cron` /
`pg_query_logging` (Postgres-only), `version` (engine version is pinned by the
plan for now). Broker-generated identifiers are validated before the AWS call
(#524).

## Naming convention (future production plans)

`oracle-19c-dev` is a **DEV-tier** plan (the `-dev` suffix mirrors the sandbox-only
`micro-psql`/`small-mysql` tier). Production plans will follow the documented RDS
family naming customers already use — `small-oracle`, `medium-oracle`,
`large-oracle`, and `*-redundant` / `*-replica` variants — and are added **only
after** (a) the live dev-RDS proof (WS15) and (b) the per-binding least-privilege
Oracle user (**#534**), which is a **pre-release blocker** for any non-dev plan
because the Oracle RDS master user holds DBA and is currently handed to every
self-service binding.

## Production plans

Not defined yet (see above). See [limitations.md](limitations.md).
