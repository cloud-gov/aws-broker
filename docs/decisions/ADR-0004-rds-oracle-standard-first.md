# ADR-0004 — Standard Amazon RDS for Oracle 19c, Standard Edition 2 + License Included

- **Status:** Accepted
- **Date:** 2026-07-17 (revised 2026-07-18)
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Related:** [#522](https://github.com/cloud-gov/aws-broker/issues/522)

## Context

Oracle on AWS can be run several ways: standard **Amazon RDS for Oracle** (managed),
**RDS Custom for Oracle** (more OS/DB access, more customer responsibility), or
self-managed Oracle on EC2. STIG hardening spans SQL-level, DB-parameter-level, and
OS/host-level controls; managed RDS makes host-level controls AWS-inherited and
inaccessible to the tenant.

Oracle licensing on RDS has two models with a hard constraint (verified against AWS
docs):

- **License Included (LI)** — AWS holds the Oracle license, bundled into the
  instance-hour. **SE2 only.**
- **Bring-Your-Own-License (BYOL)** — the customer supplies the license (+ Oracle
  Software Update License & Support). **EE or SE2.**

**Enterprise Edition is BYOL-only on RDS; License Included is Standard-Edition-2-only.**
The two are mutually exclusive: you cannot have LI + EE.

A platform-liability concern drives the decision: with BYOL, nothing technically
prevents a customer provisioning an Oracle DB **without** a valid license, and
cloud.gov does not want to facilitate an unlicensed Oracle DB running on
GSA-operated infrastructure. Under License Included there is **no unlicensed
state** — AWS holds the license and it is inseparable from the running instance.

> **Revision note:** the original ADR-0004 chose **EE + BYOL** (to keep Oracle-native
> TDE / Fine-Grained Auditing and treat licensing as the customer's responsibility).
> That was reversed on 2026-07-18: eliminating the unlicensed-Oracle liability is the
> priority, LI is SE2-only, and SE2-LI availability in GovCloud was confirmed. The
> pre-reversal decision is preserved under "Superseded decision" below.

## Decision

1. **Target standard Amazon RDS for Oracle 19c first.** Escalate to RDS Custom or
   self-managed EC2 **only if** a STIG gap analysis proves standard RDS cannot meet
   the required control posture.
2. **Edition + license model: Oracle Standard Edition 2 (SE2) + License Included**
   (`dbType: oracle-se2`, `licenseModel: license-included`). AWS holds the Oracle
   license (bundled into the instance price); there is no unlicensed state and
   cloud.gov is not in the Oracle-licensing business. The higher instance cost is
   passed through to agencies as cloud.gov resource credits (the existing RDS
   chargeback model). **Not offered:** EE / BYOL.
3. **Accept the SE2 STIG deviations.** SE2 lacks the EE-only features — most
   relevantly **Oracle-native Transparent Data Encryption (TDE)** and **Fine-Grained
   Auditing (FGA)**. These are handled as documented deviations
   ([licensing.md](../oracle19c/licensing.md)) accepted via ISSO risk/deviation:
   - **At-rest encryption** is satisfied by **RDS storage-level encryption
     (AES-256/KMS)**, which is edition-independent and already the basis for the
     Postgres/MySQL plans — not Oracle-native TDE.
   - **Auditing** relies on **standard / unified (mixed-mode) auditing** via the
     `audit_trail` parameter + CloudWatch `audit` export — not FGA.
   - **Database Vault** is unsupported on RDS **even on EE**, so it was never part
     of the posture regardless of edition.

## Consequences

- **Positive:** managed RDS gives AWS-inherited host controls (patching, OS
  hardening), a smaller broker responsibility surface, and the standard RDS
  lifecycle the broker already drives. STIG controls cleanly split into
  broker/param/option/SQL/inherited layers (mapped in the overlay).
- **Positive (LI):** **no unlicensed-Oracle liability** — AWS holds the license, so
  a running instance is licensed by construction; cloud.gov cannot facilitate an
  unlicensed DB. No BYOL entitlement, no attestation, no double-licensing of
  Multi-AZ standbys.
- **Positive (cost model):** the bundled-license premium is expressed via the
  OSBAPI plan (`free: false`, `metadata.costs`) and passed through as credits — no
  new billing machinery.
- **Negative (SE2 capability):** no Oracle-native TDE, FGA, VPD, Label Security,
  Data Redaction, or Partitioning. Mitigated/accepted as above; the residual
  TDE/FGA control interpretation is an ISSO risk/deviation decision.
- **Negative:** ~33 overlay controls are OS/listener-level and **cannot** be
  validated on RDS — they must be marked `aws_inherited` / `not_applicable_rds`
  (overlay [#1](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/1)),
  not failed.
- **Escalation trigger:** if the STIG gap analysis ([#530](https://github.com/cloud-gov/aws-broker/issues/530))
  or the ISSO finds a control that **requires** Oracle-native TDE/FGA and cannot be
  met by SE2 + RDS-KMS + standard auditing, re-open the edition decision (EE/BYOL,
  with the liability handled by attestation) as a new ADR — do not silently switch.

## Alternatives considered

- **EE + BYOL** (the superseded decision) — keeps Oracle-native TDE/FGA but is
  BYOL-only, which leaves the unlicensed-Oracle liability on GSA-operated infra
  (attestation only reallocates, does not remove it) and adds BYOL operational
  burden (entitlement + SULS, double licensing for Multi-AZ). Rejected in favor of
  removing the liability entirely.
- **SE2 + LI with an EE/BYOL "advanced" plan alongside** — rejected for now to keep
  a single, simple offering; can be revisited if a customer has a hard EE-feature
  requirement and a license.
- **RDS Custom / self-managed EC2** — rejected as the baseline: more customer/OS
  responsibility, larger STIG surface. Kept as escalation paths.

## Superseded decision (pre-2026-07-18, for history)

> **License model: BYOL**, edition **oracle-ee**. Customer solely responsible for
> holding a valid Oracle license; cloud.gov does not manage/verify/enforce it.
> Rationale at the time: keep EE-only security features (TDE/FGA) and treat
> licensing as the customer's responsibility, avoiding LI cost/availability
> uncertainty in GovCloud. Reversed once SE2-LI GovCloud availability was confirmed
> and the unlicensed-Oracle platform liability was prioritized over EE features.
