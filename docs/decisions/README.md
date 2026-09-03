# Architecture Decision Records

MADR-style decision records for `aws-broker`. Each ADR captures one decision, its
context, and its consequences. Status reflects reality on `main`:

- **Accepted** — decided and reflected in the codebase.
- **Proposed** — decided in principle but **not yet implemented** on `main`.
- **Superseded** — replaced by a later decision (noted in-file).

An ADR reaching **Accepted** is evidenced by PR approval from a team member other
than its authors, not by self-assertion.

## Oracle 19c on Amazon RDS

| ADR | Decision | Status |
| --- | --- | --- |
| [ADR-0001](ADR-0001-oracle-19c-stig-hardened-in-aws-broker.md) | Implement STIG-hardened Oracle 19c in aws-broker: SE2 + License Included, validation performed externally by the CINC/InSpec overlay | Accepted |

A **proposed** ADR covering future Cloud Service Broker portability (an
`RDSBaseline` engine abstraction + structured YAML) is raised separately, since it
is not implemented on `main`.
