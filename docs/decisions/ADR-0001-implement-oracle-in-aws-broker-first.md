# ADR-0001 — Implement Oracle 19c in aws-broker first (not a CSB brokerpak)

- **Status:** Accepted
- **Date:** 2026-07-17
- **Deciders:** cloud.gov platform team
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Consensus:** ratified 7/7 by multi-model review (2026-07-17)

## Context

Cloud.gov needs a STIG-hardened Oracle Database 19c offering on Amazon RDS
(GovCloud). Two plausible homes exist:

1. **`cloud-gov/aws-broker`** — the existing Cloud Foundry AWS service broker for
   RDS, OpenSearch, and Elasticache. RDS provisioning lifecycle, service catalog
   plans, create/update/delete/bind, parameter groups, option groups, CloudWatch
   log exports, credential generation/storage/binding, and day-2 expectations
   already live here.
2. **`cloud-gov/csb`** — the home for deployed Cloud Service Broker brokerpaks; the
   likely long-term architecture for cloud.gov brokerage.

Issue [#499](https://github.com/cloud-gov/aws-broker/issues/499) ("Add support for
Oracle databases") and [#501](https://github.com/cloud-gov/aws-broker/issues/501)
("Use Chef InSpec to validate…STIG") both frame the work against `aws-broker`.

## Decision

**Implement Oracle 19c in `cloud-gov/aws-broker` first.** Do **not** begin by
building a separate CSB brokerpak in `cloud-gov/csb`.

## Rationale

- The documented user flow (`cf create-service`, `cf bind-service`,
  `VCAP_SERVICES`, `DATABASE_URL`), RDS provisioning lifecycle, credential handling,
  and service-binding model already exist in `aws-broker`. Oracle reuses them.
- Starting in CSB would create a **second RDS operating model** before the team has
  decided to migrate existing Postgres/MySQL RDS services there — two brokers
  provisioning RDS simultaneously, with divergent credential/binding/state models.
- Oracle previously existed in `aws-broker` and was reverted; the reverted commits
  are a working template (see the current-state analysis).

## Consequences

- **Positive:** minimal new operational surface; reuses proven RDS lifecycle,
  credential encryption, and binding plumbing; consistent customer experience with
  existing RDS engines; fastest path to a testable dev offering.
- **Negative:** adds Oracle-specific complexity to `aws-broker` (mitigated by the
  `RDSBaseline` abstraction, [ADR-0003](ADR-0003-design-oracle-baseline-for-future-csb-portability.md)).
- **Follow-on:** a future migration to CSB remains open and is kept viable by
  ADR-0003; documented (not implemented) in `docs/oracle19c/future-csb-migration.md`.

## Alternatives considered

- **Build a CSB brokerpak first** — rejected: premature second RDS model; larger
  blast radius; no team decision to move RDS brokerage yet.
- **Standalone bespoke Oracle broker** — rejected: duplicates lifecycle/credential/
  binding code that already exists; explicitly a non-goal of the epic.
