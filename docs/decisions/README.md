# Architecture Decision Records

MADR-style decision records for `aws-broker`. Each ADR captures a decision, its
context, and consequences. Status reflects reality on `main`:

- **Accepted** — decided and reflected in the codebase.
- **Proposed** — decided in principle but **not yet implemented** on `main`.
- **Superseded** — replaced by a later decision (noted in-file).

## Oracle 19c on Amazon RDS

| ADR | Decision | Status |
| --- | --- | --- |
| [ADR-0001](ADR-0001-implement-oracle-in-aws-broker-first.md) | Implement Oracle 19c in aws-broker first (not a CSB brokerpak) | Accepted |
| [ADR-0002](ADR-0002-keep-stig-validation-in-overlay-repo.md) | STIG validation lives in the overlay repo; broker never runs InSpec | Accepted (JSON validation contract not yet built) |
| [ADR-0003](ADR-0003-design-oracle-baseline-for-future-csb-portability.md) | Oracle baseline behind an `RDSBaseline` engine + structured YAML | **Proposed** (main took a minimal imperative path in #564; open decision — #568) |
| [ADR-0004](ADR-0004-rds-oracle-standard-first.md) | Standard RDS for Oracle, SE2 + License Included | Accepted (audit compensating control pending param-group hardening) |
| [ADR-0005](ADR-0005-local-testing-is-development-signal-only.md) | Local testing is development signal only, never compliance evidence | Accepted |

> **Provenance:** these ADRs were authored by Peter Burkholder on the
> `peterb/sql-hardening-adr` branch (2026-07-17/18). They are landed here on `main`
> with **status corrections** so the record does not over-claim the current
> architecture — specifically ADR-0003 is marked *Proposed* (the engine abstraction
> is not on `main`) and the ADR-0002 JSON-contract detail is annotated as not-yet-built.
> Remaining implementation is tracked in
> [#568](https://github.com/cloud-gov/aws-broker/issues/568).
