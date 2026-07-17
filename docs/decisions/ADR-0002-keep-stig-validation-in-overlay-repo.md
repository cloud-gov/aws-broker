# ADR-0002 — Keep STIG validation in the overlay repo, not in aws-broker

- **Status:** Accepted
- **Date:** 2026-07-17
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Related:** [#501](https://github.com/cloud-gov/aws-broker/issues/501),
  [cg-oracle-database-19c-stig-overlay#1](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/1)

## Context

Oracle 19c must be hardened to the DISA Oracle 19c STIG and the posture must be
*validated with evidence*. There is an existing validation asset:
[`cloud-gov/cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay)
— a CINC/InSpec profile that connects to Oracle via the `oracledb_session` resource
(SQL\*Plus) and is already architected to reach a brokered AWS RDS Oracle instance
through a `cf ssh` tunnel.

`aws-broker` is a Go service broker. It has no InSpec/Cinc runtime, and adding one
would couple provisioning logic to compliance-checking logic.

## Decision

**The broker provisions and configures; the overlay validates.** STIG validation,
InSpec/Cinc controls, SQL assessment, RDS-applicability mapping, and evidence
generation live in `cg-oracle-database-19c-stig-overlay`. **`aws-broker` never runs
InSpec/Cinc.**

The interface between them is a **broker-produced validation contract** (a JSON
document describing the provisioned instance — endpoint, engine, parameter/option
groups, log exports, credential *references*, never secrets) that the overlay
consumes ([#530](https://github.com/cloud-gov/aws-broker/issues/530)).

## Consequences

- **Positive:** clean separation of concerns / separation of duties (the thing
  being audited does not audit itself); the broker stays a broker; the overlay
  can evolve controls independently; no InSpec/Ruby dependency in a Go broker.
- **Positive (security):** no self-attestation — evidence is produced by an
  independent tool.
- **Negative:** a contract surface must be defined and kept in sync between the two
  repos (mitigated by a versioned JSON schema).
- **Obligation:** SQL-level hardening scripts live in (or are consumed by) the
  overlay, not tangled into broker runtime
  ([#529](https://github.com/cloud-gov/aws-broker/issues/529)).

## Alternatives considered

- **Run InSpec inside the broker** — rejected: couples provisioning to compliance;
  adds a Ruby/InSpec runtime to a Go service; self-attestation smell.
- **Duplicate STIG logic in the broker as Go checks** — rejected: divergence from
  the authoritative DISA profile; double maintenance.
