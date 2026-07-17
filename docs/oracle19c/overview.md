# Oracle 19c brokered RDS — overview

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16
> ([#532](https://github.com/cloud-gov/aws-broker/issues/532)).
> **Status: dev/test, not production-ready.** No live GovCloud RDS proof has run
> yet (gated, WS15).

## What it is

A STIG-hardened **Oracle Database 19c** offering delivered through the existing
Cloud.gov `aws-broker` as an RDS service plan (`oracle-19c-dev`). A tenant does:

```
cf create-service aws-rds oracle-19c-dev my-oracle
cf bind-service my-app my-oracle
```

and receives Oracle connection details in `VCAP_SERVICES`. The broker provisions a
private, encrypted, **born-hardened** RDS Oracle instance; STIG posture is validated
out-of-band by the [`cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay).

## Key properties

| Property | Value | Why |
|----------|-------|-----|
| Engine | `oracle-ee` 19c | Enterprise Edition ([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)) |
| License | **BYOL** (bring-your-own-license) | GovCloud policy ([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md), gate [#536](https://github.com/cloud-gov/aws-broker/issues/536)) |
| Encryption | at rest (KMS) | STIG / SC-28 |
| Network | private only | [ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md); no public accessibility |
| Parameter group | broker-managed hardened baseline (born hardened) | [ADR-0003](../decisions/ADR-0003-design-oracle-baseline-for-future-csb-portability.md), [#525](https://github.com/cloud-gov/aws-broker/issues/525) |
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
