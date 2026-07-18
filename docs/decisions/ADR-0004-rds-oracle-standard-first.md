# ADR-0004 — Target standard Amazon RDS for Oracle 19c first; license model BYOL

- **Status:** Accepted
- **Date:** 2026-07-17
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Related:** [#522](https://github.com/cloud-gov/aws-broker/issues/522),
  [#536](https://github.com/cloud-gov/aws-broker/issues/536)

## Context

Oracle on AWS can be run several ways: standard **Amazon RDS for Oracle** (managed),
**RDS Custom for Oracle** (more OS/DB access, more customer responsibility), or
self-managed Oracle on EC2. STIG hardening spans SQL-level, DB-parameter-level, and
OS/host-level controls; managed RDS makes host-level controls AWS-inherited and
inaccessible to the tenant.

Oracle licensing on RDS is either **License Included** (not available for all
editions/regions, and a cloud.gov cost/policy question) or **Bring-Your-Own-License
(BYOL)**.

## Decision

1. **Target standard Amazon RDS for Oracle 19c first.** Escalate to RDS Custom or
   self-managed EC2 **only if** a STIG gap analysis proves standard RDS cannot meet
   the required control posture.
2. **License model: BYOL** (`licenseModel: bring-your-own-license`). The customer
   is solely responsible for holding a valid Oracle license; **cloud.gov does not
   manage, supply, verify, or enforce it** and does not gate provisioning on
   license evidence. Customer instructions live in
   [licensing.md](../oracle19c/licensing.md).
3. **Edition:** default `oracle-ee` for 19c unless a SE2 gap analysis shows SE2
   suffices; the choice is recorded in the catalog plan and can be revisited.

## Consequences

- **Positive:** managed RDS gives AWS-inherited host controls (patching, OS
  hardening), a smaller broker responsibility surface, and the standard RDS
  lifecycle the broker already drives. STIG controls cleanly split into
  broker/param/option/SQL/inherited layers (mapped in the overlay).
- **Positive (BYOL):** avoids License-Included availability/cost uncertainty in
  GovCloud; the customer owns the license (and the responsibility for it), which is
  the correct boundary — cloud.gov is not in the Oracle-licensing business.
- **Negative:** ~33 overlay controls are OS/listener-level and **cannot** be
  validated on RDS — they must be marked `aws_inherited` / `not_applicable_rds`
  (overlay [#1](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/1)),
  not failed.
- **Negative (BYOL):** the customer must arrange their own Oracle entitlement
  before use; we provide clear instructions ([licensing.md](../oracle19c/licensing.md))
  rather than a broker-enforced gate.
- **Escalation trigger:** if the STIG gap analysis ([#530](https://github.com/cloud-gov/aws-broker/issues/530))
  finds required controls that standard RDS cannot satisfy, re-open the RDS-Custom
  vs EC2 decision as a new ADR.

## Alternatives considered

- **RDS Custom for Oracle first** — rejected as the baseline: more customer/OS
  responsibility, larger STIG surface for the broker, contradicts the managed-first
  posture. Kept as an escalation path.
- **Self-managed EC2 Oracle** — rejected: maximal responsibility/attack surface;
  only if both RDS and RDS Custom prove insufficient.
- **License Included** — rejected as default: availability/cost uncertainty in
  GovCloud; BYOL keeps license ownership explicit.
