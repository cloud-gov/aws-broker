# ADR-0001 — Implement STIG-hardened Oracle 19c in aws-broker

- **Status:** Accepted
- **Date:** 2026-07-17 (licensing revised 2026-07-18; consolidated 2026-08-31)
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)

## Context

Cloud.gov needs a STIG-hardened Oracle Database 19c offering on Amazon RDS
(GovCloud). Three questions had to be settled together: where the broker code
lives, which Oracle edition and license model to offer, and where STIG validation
runs.

## Decision

Implement Oracle 19c in **`cloud-gov/aws-broker`**, on **standard Amazon RDS for
Oracle, Standard Edition 2 with License Included**, with **STIG validation
performed externally** by the CINC/InSpec overlay plus SQL-based hardening.

1. **`aws-broker`, not a CSB brokerpak.** The RDS provisioning lifecycle, catalog
   plans, credential generation/binding, parameter and option groups, and
   CloudWatch log exports already exist here; Oracle reuses them. Starting in
   `cloud-gov/csb` would stand up a second RDS operating model before the team has
   decided to migrate the existing Postgres/MySQL RDS services.
2. **SE2 + License Included** (`dbType: oracle-se2`,
   `licenseModel: license-included`). See *Licensing* below — this is a liability
   decision, not a cost preference.
3. **The broker provisions; the overlay validates.** `aws-broker` never runs
   InSpec/Cinc. STIG controls, SQL assessment, RDS-applicability mapping, and
   evidence generation live in
   [`cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay).
   Separation of duties: the thing being audited does not audit itself.

## Licensing: why SE2 + License Included

Oracle on RDS has two license models with a hard constraint: **License Included is
SE2-only; Enterprise Edition is BYOL-only.** They are mutually exclusive — LI + EE
is not available.

The deciding factor is platform liability. Under **BYOL**, nothing technically
prevents a tenant provisioning Oracle **without** a valid license, and cloud.gov
does not want to facilitate an unlicensed Oracle database on GSA-operated
infrastructure. Attestation reallocates that liability but does not remove it.
Under **License Included there is no unlicensed state** — AWS holds the license and
it is inseparable from the running instance. The bundled-license premium is passed
through as resource credits via the existing RDS chargeback model.

## Accepted SE2 deviations and compensating controls

SE2 lacks EE-only features. These are documented deviations accepted via ISSO
risk/deviation, **not** unmet controls:

| Missing EE feature | Compensating control | Status |
| --- | --- | --- |
| Oracle-native Transparent Data Encryption (TDE) | **RDS storage-level encryption** (AES-256/KMS, `encrypted: true`) — edition-independent, and already the basis for the Postgres/MySQL plans | In effect on `main` |
| Fine-Grained Auditing (FGA) | **Unified auditing** (RDS runs mixed mode), enabled by the `audit_trail` parameter | **Intended, not yet on `main`** — the Oracle parameter-group baseline is pending (#568) |

Oracle Database Vault is unsupported on RDS **even on EE**, so it was never part of
the posture regardless of edition.

> **Open question — do not read the auditing row as settled.** On RDS, CloudWatch
> log export ships only the OS `.aud`/`.xml` files; records written with
> `audit_trail=DB`/`DB,EXTENDED` stay in-database and are **not** exportable, and
> the unified trail (`AUDSYS.AUD$UNIFIED`) is not an OS file either. Getting the
> *unified* trail off-box requires Database Activity Streams, which is out of scope.
> The off-box mechanism and the correct `audit_trail` value are therefore still
> being decided — see
> [overlay#48](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/48).
> This ADR records that auditing is the FGA compensating control; it does **not**
> assert that audit records currently reach a central log facility.

Consequence to plan for: roughly 33 overlay controls are OS/listener-level and
**cannot** be validated on managed RDS. They must be dispositioned
`aws_inherited` / `not_applicable_rds`, not failed.

## Escalation trigger

**If the ISSO or a gap analysis identifies a control that *requires* Oracle-native
TDE or FGA and cannot be met by SE2 + RDS-KMS + standard/unified auditing, re-open
the edition decision as a new ADR. Do not silently switch to EE/BYOL.**

Likewise, if the team decides to migrate RDS brokerage to the CSB, that is a new
decision — see the proposed CSB-portability ADR.

## Superseded sub-decision (history)

The original 2026-07-17 decision chose **EE + BYOL**, to retain Oracle-native
TDE/FGA and treat licensing as the customer's responsibility. It was **reversed on
2026-07-18** once SE2-LI availability in GovCloud was confirmed and the
unlicensed-Oracle platform liability was prioritised over EE features. Recorded so
the reversal is discoverable and is not re-litigated by accident.

## Alternatives considered

- **CSB brokerpak first** — rejected: premature second RDS operating model, larger
  blast radius, no team decision to move RDS brokerage yet.
- **Standalone bespoke Oracle broker** — rejected: duplicates lifecycle,
  credential, and binding code that already exists.
- **EE + BYOL** — rejected: see *Licensing* (unlicensed-Oracle liability on
  GSA-operated infrastructure; plus BYOL entitlement/SULS burden and double
  licensing for Multi-AZ standbys).
- **SE2 + LI alongside an EE/BYOL "advanced" plan** — deferred: keeps one simple
  offering; revisit only under the escalation trigger above.
- **RDS Custom / self-managed EC2** — rejected as the baseline: more customer/OS
  responsibility, larger STIG surface. Retained as escalation paths.
- **Run InSpec inside the broker** — rejected: couples provisioning to compliance,
  adds a Ruby/InSpec runtime to a Go service, and is self-attestation.
- **Re-implement STIG checks as Go checks in the broker** — rejected: diverges from
  the authoritative DISA profile and doubles maintenance.

## Consequences

- **Positive:** reuses proven RDS lifecycle, credential encryption, and binding
  plumbing; consistent customer experience with existing RDS engines; AWS-inherited
  host controls (patching, OS hardening) shrink the broker's responsibility
  surface; no unlicensed-Oracle liability; evidence is produced by an independent
  tool rather than self-attested.
- **Negative:** adds Oracle-specific complexity to `aws-broker`; no Oracle-native
  TDE, FGA, VPD, Label Security, Data Redaction, or Partitioning on SE2; the
  broker↔overlay boundary must be kept in sync by hand.
- **Follow-on:** a future CSB migration remains open (separate proposed ADR).
  Remaining Oracle hardening is tracked in
  [#568](https://github.com/cloud-gov/aws-broker/issues/568); the live GovCloud
  validation run in [#558](https://github.com/cloud-gov/aws-broker/issues/558).
