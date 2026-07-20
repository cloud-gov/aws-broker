# Oracle 19c — licensing (SE2 + License Included)

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519).
>
> **The Oracle license is included.** The `oracle-se2-license-included-dev` plan
> uses Amazon RDS **License Included**: AWS holds the Oracle Database license and
> bundles it into the instance price. Customers do **not** buy or bring an Oracle
> license, and there is **no unlicensed state** — cloud.gov never facilitates an
> unlicensed Oracle database.

## Why License Included (and why Standard Edition 2)

- **No unlicensed-Oracle risk.** Under License Included the Oracle license is held
  by AWS and inseparable from the running instance
  ([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)). A customer
  cannot spin up an unlicensed Oracle DB on cloud.gov-operated infrastructure.
- **License Included is Standard Edition 2 (SE2) only** on RDS — Enterprise Edition
  is BYOL-only. Choosing License Included to remove the licensing liability means
  the offering is **SE2**.
- **Cost.** The bundled license makes an Oracle plan cost more than an
  open-source-engine plan. That cost is expressed on the plan (`free: false`,
  `metadata.costs`) and passed through to agencies as **cloud.gov resource
  credits**, the same chargeback model used for the Postgres/MySQL RDS plans.
  *(The exact GovCloud SE2-LI rate is a `TODO(cost)` in the catalog pending
  confirmation from AWS GovCloud pricing.)*

## What the customer needs to do

**Nothing, for licensing.** Provision the service like any other RDS plan:

```
cf create-service aws-rds oracle-se2-license-included-dev my-oracle
cf bind-service my-app my-oracle
```

No license key, no BYOL entitlement, no attestation, no license parameter. The
price already includes the Oracle license.

## SE2 STIG deviations (documented; ISSO risk/deviation)

Standard Edition 2 does not include the Enterprise-Edition-only security features.
For a STIG-hardened offering the material gaps, and how they are handled, are:

| EE-only feature (absent in SE2) | Handling on this offering |
|---|---|
| **Oracle-native TDE** (tablespace / column encryption) | **RDS storage-level encryption at rest (AES-256 / KMS)** is enabled (`encrypted: true`) — edition-independent, the same at-rest control used by the Postgres/MySQL plans. Oracle-native in-database TDE is not available on SE2. |
| **Fine-Grained Auditing (FGA)** | **Standard / unified (mixed-mode) auditing** via the `audit_trail` parameter + CloudWatch `audit` export (the born-hardened baseline). FGA (policy/predicate-level) is not available on SE2 — and even on EE, RDS does not export FGA events to CloudWatch. |
| Oracle Database Vault | **Not applicable** — unsupported on RDS even for EE. |
| VPD, Label Security, Data Redaction, Partitioning, Data Guard | Not available on SE2; not part of this offering's control posture. |

Whether a specific STIG control is satisfied by **RDS-KMS at-rest encryption** in
place of **Oracle-native TDE**, and by **standard/unified auditing** in place of
**FGA**, is a control-interpretation decision recorded as an **ISSO risk/deviation
acceptance**. If a control is found to *require* Oracle-native TDE or FGA, the
edition decision (EE + BYOL, with the licensing liability handled by attestation)
must be re-opened as a new ADR — see ADR-0004's escalation trigger.

## Why the broker still has an `ENABLE_ORACLE` switch (not a license control)

Oracle provisioning is disabled unless the operator sets `ENABLE_ORACLE`. This is a
**staged-rollout switch** for a new offering not yet validated on a live foundation
— not a licensing or security control. Licensing is handled entirely by License
Included (above); the switch just governs when the offering appears while it is
being validated.

## References

- [ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md) — SE2 + License Included decision + SE2 deviations
- [service-plans.md](service-plans.md) — the `oracle-se2-license-included-dev` plan
- AWS: *Licensing Amazon RDS for Oracle* (License Included is SE2-only) — vendor docs
- AWS: *Amazon RDS for Oracle pricing* (License Included bundles the Oracle license)
