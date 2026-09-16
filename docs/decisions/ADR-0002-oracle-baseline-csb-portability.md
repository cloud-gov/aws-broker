# ADR-0002 — Design the Oracle baseline for future Cloud Service Broker portability

- **Status:** **Rejected** (2026-09-11) — proposed 2026-08-31, declined on review
- **Date:** 2026-07-17 (raised 2026-08-31; rejected 2026-09-11)
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Decision-makers:** @markdboyd (rejecting rationale), @wz-gsa (author)
- **Superseded by:** nothing — see *Revisiting* below

## Context

[ADR-0001](ADR-0001-oracle-19c-stig-hardened-in-aws-broker.md) decided to implement
Oracle 19c in `aws-broker` rather than as a Cloud Service Broker brokerpak, and
noted that a future migration to `cloud-gov/csb` remains open. That migration is not
scheduled and may never happen.

The Oracle work that has landed took a deliberately **minimal, imperative** path:
[#564](https://github.com/cloud-gov/aws-broker/pull/564) added
`services/rds/oracle_tls.go` for the SSL option group, with Oracle specifics written
directly into Go. Subsequent hardening (parameter-group baseline, log exports)
followed the same shape.

The question this ADR raised: should Oracle's STIG-relevant configuration be
expressed as **declarative data behind an engine abstraction**, so a CSB brokerpak
could consume it later without re-deriving the hardening posture?

## Decision — rejected

**Do not introduce an `RDSBaseline` engine abstraction for CSB portability.** Keep
`aws-broker`'s RDS code as imperative Go.

The proposal was to express each engine's STIG-relevant configuration (parameter
groups, option groups, log exports, plan constraints) as structured data behind an
engine interface, on the argument that the hardening posture *is* data and would then
be reviewable, diffable against a scan result, and reusable by a second consumer.

That argument was not persuasive enough to justify the cost, for the reason recorded
below.

## Rationale for rejection

> "A migration of the AWS broker to the CSB may or may not ever happen. I understand
> that the ADR is saying that we should leave the option open for a future CSB
> migration, but I don't think the effort is worthwhile, especially when the currently
> imperative Golang code allows us to move faster. While this broker remains, I think
> its code should be maintained in a consistent way as imperative Golang code.
> Introducing new abstractions that have no immediate value for a possible future
> migration just doesn't seem worthwhile."
>
> — @markdboyd, [#575 review](https://github.com/cloud-gov/aws-broker/pull/575)

Three points, in order of weight:

1. **The second consumer does not exist and may never.** An abstraction whose only
   justification is a hypothetical future caller is speculative generality. The
   portability benefit is unrealised unless a CSB migration is actually decided.
2. **Codebase consistency has present value; portability has contingent value.**
   While this broker is the RDS brokering path, its code should look like one thing.
   A data-driven engine layer for Oracle alongside imperative Go for Postgres/MySQL
   would leave two idioms in `services/rds/` indefinitely.
3. **Velocity.** The imperative path is shipping working hardening now. A refactor of
   working code, paid for up front against a contingent benefit, slows that down.

This ADR anticipated its own rejection — it stated that accepting it required an
undecided migration question, called the abstraction "speculative generality," and
required an explicit disposition rather than being left indefinitely Proposed. The
review supplied the reason. Recording it as Rejected is the disposition.

## Accepted cost of rejecting

**If a CSB migration is later approved, the Oracle hardening posture must be
re-derived from imperative Go**, with the attendant risk that a STIG-relevant setting
is dropped in translation. That cost is accepted, and it is bounded by two things:

- The DISA STIG and the
  [`cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay)
  remain the authoritative statement of the required posture, independent of this
  broker's code. A re-derivation can be validated against the overlay rather than
  reverse-engineered from Go.
- The long-lived branch `feat/oracle-19c-stig-brokered-rds` already carries per-engine
  YAML (`services/rds/baselines/oracle19c/parameters.yml`, `options.yml`,
  `log_exports.yml`) loaded by `services/rds/baselines.go`. That is baseline *data*
  without an engine *interface* — it captures much of the reviewability benefit and is
  **not** rejected by this ADR. Rejection applies to the `RDSBaseline` abstraction,
  not to keeping hardened values in reviewable YAML.

## Revisiting

Reopen as a **new** ADR, not by editing this one, if either becomes true:

- Cloud.gov decides to migrate RDS brokerage to the CSB, giving the abstraction a
  real second consumer; or
- a second engine needs the same STIG-baseline treatment, making the abstraction
  justified by present duplication rather than future portability.

Absent one of those, this decision stands.
