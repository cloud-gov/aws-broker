# ADR-0003 — Design the Oracle baseline for future CSB portability

- **Status:** Accepted
- **Date:** 2026-07-17
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Related:** [ADR-0001](ADR-0001-implement-oracle-in-aws-broker-first.md),
  [#523](https://github.com/cloud-gov/aws-broker/issues/523)

## Context

ADR-0001 puts Oracle 19c in `aws-broker` first, but `cloud-gov/csb` (Cloud Service
Broker brokerpaks) remains the likely long-term home if cloud.gov migrates RDS
brokerage to CSB. The current-state analysis found the broker has **no per-engine
abstraction** — engine behavior is ~15 scattered `if i.DbType == "..."` sites.
Piling Oracle STIG hardening on top of that pattern would create a mess that is
both hard to review and hard to migrate.

## Decision

Implement Oracle hardening behind a clean **`RDSBaseline` engine-strategy interface**
and express the hardened values as **structured baseline files** (YAML) rather than
hard-coded Go conditionals.

- Interface (`services/rds`): `Engine`, `AppliesTo`, `NeedsCustomParameterGroup`,
  `DefaultParameters`, `NeedsCustomOptionGroup`, `DefaultOptions`, `EnabledLogExports`,
  `ValidateCreateParams`, `ValidateUpdateParams`, `BindingFormat`, `RequiresReboot`.
- Implementations: `mysqlBaseline`, `postgresBaseline`, `oracle19cBaseline`.
- Structured data: `services/rds/baselines/oracle19c/{parameters,options,log_exports,binding,validation}.yml`.
- Existing engines are refactored behind the interface **without behavior change**;
  the existing MySQL/Postgres regression tests must stay green.

## Consequences

- **Positive:** Oracle behavior is isolated and reviewable; hardened values live in
  data files a security reviewer can read; the branching inventory collapses into
  one dispatch point; the baseline files are directly reusable by a future CSB
  brokerpak (which is also declarative + Terraform/OpenTofu-driven).
- **Positive:** future migration to CSB does not require re-deriving the hardened
  parameter/option/log baselines.
- **Negative:** a refactor of working code carries regression risk (mitigated by
  behavior-preserving refactor + green regression suite as an acceptance gate).
- **Boundary:** broker *behavior* (async River lifecycle, credential encryption,
  gorm state) is **not** portable and is out of scope for the baseline files; that
  gap is documented in the future-CSB migration doc.

## Alternatives considered

- **Add Oracle as more `if DbType==` branches** — rejected: worsens an existing code
  smell, scatters STIG-relevant values, hard to review and migrate.
- **Build the abstraction only for Oracle, leave pg/mysql as-is** — rejected: leaves
  the dispatch inconsistent and the branching inventory only partially resolved.
