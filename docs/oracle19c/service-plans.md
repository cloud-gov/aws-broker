# Oracle 19c — service plans

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

Oracle is offered as a plan on the existing `aws-rds` service (not a separate
service), so it shares the RDS lifecycle, credential, and binding model.

## `oracle-se2-license-included-dev` (dev/test)

Defined in `catalog-template.yml` / `catalog-test.yml`:

| Field | Value |
|-------|-------|
| `dbType` | `oracle-se2` |
| `dbVersion` | `19.0.0.0.ru-2024-07.rur-2024-07.r1` (pinned) |
| `approvedMajorVersions` | `["19"]` |
| `licenseModel` | `license-included` |
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

> **Staged rollout.** Oracle provisioning is disabled unless the broker environment
> sets `ENABLE_ORACLE` (`EnableOracleFeature`). This is a rollout control for a new
> offering not yet validated on a live foundation — **not** a security/boundary
> control. Oracle uses the same credential model as the postgres/mysql plans
> (master credential per binding; the customer creates their own least-privilege
> in-database users). `cf create-service … oracle-se2-license-included-dev …` returns an
> opt-in-required error when the flag is unset.

> **License Included — Oracle license bundled by AWS.** This plan is
> `license-included`: AWS holds the Oracle Database license and bundles it into the
> instance price, so there is **no unlicensed state** — cloud.gov never facilitates
> an unlicensed Oracle DB. Customers do not buy or bring an Oracle license. The
> bundled-license cost is passed through to agencies as cloud.gov resource credits.
> License Included is Standard Edition 2 (SE2) only on RDS. Details:
> [licensing.md](licensing.md).

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

`oracle-se2-license-included-dev` is a **DEV-tier** plan (the `-dev` suffix mirrors the sandbox-only
`micro-psql`/`small-mysql` tier). Production plans will follow the documented RDS
family naming customers already use — `small-oracle-se2-license-included`,
`medium-oracle-se2-license-included`, `large-oracle-se2-license-included`, and
`*-redundant` / `*-replica` variants — and are added **only
after** the live dev-RDS proof (WS15) confirms the parameter/option/log posture on
the real engine. Production plans use the same credential model as the other RDS
plans (master credential per binding; customer creates least-privilege users).

## Production plans

Not defined yet (see above). See [limitations.md](limitations.md).
