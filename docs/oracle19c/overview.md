# Oracle 19c brokered RDS — overview

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16
> ([#532](https://github.com/cloud-gov/aws-broker/issues/532)).
> **Status: dev/test, not production-ready.** No live GovCloud RDS proof has run
> yet (gated, WS15).

## What it is

A STIG-hardened **Oracle Database 19c** offering delivered through the existing
Cloud.gov `aws-broker` as an RDS service plan (`oracle-se2-license-included-dev`). It is consumed
**self-service by application developers** — not Cloud Foundry operators — exactly
like the documented `micro-psql` / `small-mysql` plans:

```
cf create-service aws-rds oracle-se2-license-included-dev my-oracle
cf bind-service my-app my-oracle
cf bind-security-group trusted_local_networks_egress <ORG> --space <SPACE>   # open egress
```

The broker provisions a private, encrypted, **born-hardened** RDS Oracle instance;
STIG posture is validated out-of-band by the
[`cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay).

### Self-service model (what the customer can/can't do)

- **Can**, without an operator: create/update/bind/unbind, rotate credentials
  (`-c '{"rotate_credentials": true}'`, then re-bind + restage — same flow and
  downtime caveat as the other RDS engines; see
  [ops/oracle19c/credential-rotation.md](../../ops/oracle19c/credential-rotation.md)),
  open space egress, tunnel via `cf ssh`, and set the allowlisted `-c` params
  (`storage`, `backup_retention_period`, `storage_type`,
  `enable_cloudwatch_log_groups_exports`). See [service-plans.md](service-plans.md).
- **Cannot** (by design): get `SYS`/`SYSDBA` (RDS gives a master user); pass
  out-of-allowlist or MySQL/Postgres-only `-c` params (rejected fail-closed, #535);
  reach the DB directly from a laptop (tunnel required); force a reboot for
  pending-reboot parameter changes or restore a backup — those go through
  cloud.gov support, same as psql/mysql. (The broker's `#535` allowlist prevents
  *weakening the baseline via broker params*; it does not stop an app that binds to
  the DBA-class master from altering the DB in-session — hence the least-privilege
  guidance below.)
- **Operator-controlled**: whether the Oracle offering is switched on at all
  (`ENABLE_ORACLE`, a platform rollout switch — not a per-request approval).

## Key properties

| Property | Value | Why |
|----------|-------|-----|
| Engine | `oracle-se2` 19c | Standard Edition 2 ([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)) |
| License | **License Included** (bundled by AWS) — no unlicensed state, cost passed through as credits ([licensing.md](licensing.md)) | [ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md) |
| Encryption | at rest (KMS) | STIG / SC-28 |
| Network | private only | [ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md); no public accessibility |
| Parameter group | broker-managed hardened baseline (born hardened) | [ADR-0003](../decisions/ADR-0003-design-oracle-baseline-for-future-csb-portability.md), [#525](https://github.com/cloud-gov/aws-broker/issues/525) |
| Option group | none at provision yet (follow-up) | [#526](https://github.com/cloud-gov/aws-broker/issues/526) |
| Log exports | `alert`, `audit`, `listener` by default | audit generation, [#527](https://github.com/cloud-gov/aws-broker/issues/527) |
| SID / DBName | `ORCL` (fixed, ≤8 upper) | RDS Oracle constraint |
| Port | 1521 | Oracle default |

## Document map

- [`service-plans.md`](service-plans.md) — plan catalog + parameters
- [`provisioning.md`](provisioning.md) — how provisioning works (async lifecycle)
- [`binding.md`](binding.md) + [`connection-examples.md`](connection-examples.md) — bind + connect (JDBC / SQLcl)
- [`hardening-baseline.md`](hardening-baseline.md) — what is hardened, and where
- [`validation.md`](validation.md) — running the STIG overlay + the validation contract
- [`limitations.md`](limitations.md) — known limitations & caveats
- [`../decisions/`](../decisions/) — ADRs 0001–0005
- [`current-state-analysis.md`](current-state-analysis.md) — how aws-broker RDS works
- [`future-csb-migration.md`](future-csb-migration.md) — portability to CSB

## Layered responsibility (who hardens what)

```
broker (aws-broker)        provisions private+encrypted RDS; hardened parameter
                           group; default audit log exports; binding creds
RDS / AWS platform         host OS, patching, listener — AWS-inherited controls
SQL hardening (overlay)    profiles, default-account lockout, unified audit
                           policies; detect-first PUBLIC-grant review
STIG overlay (InSpec)      validates all of the above; control->layer map marks
                           inherited/NA controls so RDS results are not misleading
```
