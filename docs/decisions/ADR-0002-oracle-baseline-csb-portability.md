# ADR-0002 — Design the Oracle baseline for future Cloud Service Broker portability

- **Status:** Proposed — **not implemented on `main`**
- **Date:** 2026-07-17 (raised separately 2026-08-31)
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Tracking:** [#568](https://github.com/cloud-gov/aws-broker/issues/568)

## Context

[ADR-0001](ADR-0001-oracle-19c-stig-hardened-in-aws-broker.md) decided to implement
Oracle 19c in `aws-broker` rather than as a Cloud Service Broker brokerpak, and
noted that a future migration to `cloud-gov/csb` remains open. That migration is not
scheduled and may never happen.

The Oracle work that has landed so far took a deliberately **minimal, imperative**
path: [#564](https://github.com/cloud-gov/aws-broker/pull/564) added
`services/rds/oracle_tls.go` for the SSL option group, with Oracle specifics written
directly into Go. Subsequent hardening (parameter-group baseline, log exports) is
following the same shape.

The question this ADR raises: should Oracle's STIG-relevant configuration be
expressed as **declarative data behind an engine abstraction**, so it could be
consumed by a CSB brokerpak later without re-deriving the hardening posture?

## Proposed decision

Introduce an **`RDSBaseline` engine abstraction** with each engine's STIG-relevant
configuration expressed as **structured YAML** (parameter-group settings, option
groups, log exports, plan constraints) rather than imperative Go.

Rationale: the hardening posture is *data* — a list of parameters with values,
apply-methods, and STIG intent. Expressed as data it can be validated, diffed
against a scan result, reviewed by an ISSO without reading Go, and read by a second
consumer (a brokerpak) without a rewrite. Expressed as imperative Go it is coupled
to this broker's control flow.

## Status: why this is Proposed and not Accepted

**`main` does not do this.** The engine abstraction does not exist on `main`;
Oracle configuration there is imperative Go (`services/rds/oracle_tls.go`).

The long-lived integration branch `feat/oracle-19c-stig-brokered-rds` has moved
partway toward this decision — it carries `services/rds/baselines.go` plus
per-engine YAML (`services/rds/baselines/oracle19c/parameters.yml`, `options.yml`,
`log_exports.yml`) — but that has not merged to `main`, and it is baseline *data*
loading rather than the `RDSBaseline` engine interface this ADR proposes. This ADR
records the intent and the reasoning so the option stays open; it does **not**
describe current `main` architecture.

Accepting it requires a decision that is not yet made: whether Cloud.gov is
migrating RDS brokerage to the CSB at all. Building a portability abstraction for a
migration that may not happen is speculative generality, and the honest current
position is that the incremental imperative path is shipping working hardening
faster.

## Consequences if accepted

- **Positive:** hardening posture becomes reviewable data rather than code; a CSB
  brokerpak could consume the same baseline; parameter drift becomes diffable
  against overlay scan output.
- **Negative:** a refactor of already-working code, against a migration that has not
  been decided. Adds an abstraction layer whose second consumer does not exist —
  the classic premature-generalisation risk.
- **Cost of deferring:** if the CSB migration is later approved, the Oracle posture
  must be re-derived from imperative Go, with the risk that a STIG-relevant setting
  is dropped in translation.

## Alternatives considered

- **Keep the imperative path** (current `main`) — simplest, ships fastest, no
  speculative abstraction. Cost is paid only if a CSB migration happens.
- **Build the CSB brokerpak now** — rejected in ADR-0001: premature second RDS
  operating model.
- **Extract the baseline data without an engine abstraction** — a middle option
  worth considering, and the one the integration branch has effectively taken:
  per-engine YAML under `services/rds/baselines/` may capture most of the
  portability benefit without an interface refactor.

## Decision needed

This ADR should be **Accepted, rejected, or superseded** as part of deciding the
CSB migration question — not left indefinitely Proposed. If the migration is ruled
out, this should be marked **Rejected** with that reason recorded.
