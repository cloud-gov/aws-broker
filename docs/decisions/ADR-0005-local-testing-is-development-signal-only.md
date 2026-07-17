# ADR-0005 — Local testing is development signal only, not compliance evidence

- **Status:** Accepted
- **Date:** 2026-07-17
- **Epic:** [#519](https://github.com/cloud-gov/aws-broker/issues/519)
- **Related:** [#529](https://github.com/cloud-gov/aws-broker/issues/529)

## Context

Fast iteration on Oracle SQL hardening, InSpec control logic, and broker
control-flow requires local testing without provisioning real GovCloud RDS. The
available local tools have known fidelity gaps (researched 2026-07-17):

- **moto** (free) mocks the RDS **control plane** only — no Oracle engine; it tracks
  create/describe/delete/parameter-group/option-group state but does not apply them
  to a database. (`create_db_subnet_group` is unimplemented.)
- **LocalStack** RDS is paid (Base+) and does **not** emulate an Oracle engine.
- **`gvenzl/oracle-free`** (native arm64) and a self-built **Oracle 19c EE** arm64
  image run real Oracle locally, but differ from RDS in edition/version, and give
  `SYS`/`SYSDBA` access that RDS does **not** grant (RDS gives a master user).
- Local containers cannot reproduce KMS, CloudWatch log exports, GovCloud
  networking/partition, subnet/security groups, or the real RDS async state machine.

## Decision

**Local test results are development signal only and are never compliance evidence.**

- Local reports (SQL assessment/hardening diffs, InSpec runs against local Oracle,
  moto-based broker flow tests) must be **clearly labeled** as development signal.
- Compliance evidence is produced **only** by running the overlay against a real
  brokered GovCloud RDS Oracle instance (the gated dev-RDS proof, out of scope for
  the first PR).
- To reduce false confidence, SQL hardening is developed and tested as a
  **non-`SYS`** privileged user, mirroring the RDS master-user privilege model.

## Consequences

- **Positive:** fast local iteration on the parts that *are* faithfully testable
  (SQL idempotency, InSpec control logic, broker Go unit tests, broker control-flow
  vs moto) without AWS cost or credentials.
- **Positive:** prevents the "moto/oracle-free passed ⇒ STIG-compliant" fallacy.
- **Negative:** the authoritative posture is unknown until the gated GovCloud smoke
  run; edition/privilege drift can hide RDS-only failures (mitigated by the 19c-EE
  fidelity image + non-SYS testing).
- **Boundary:** KMS, CloudWatch exports, GovCloud networking, and real RDS
  parameter/option-group *effects* are validated only in the smoke environment.

## Local test stack (Apple Silicon)

| Layer | Tool |
|-------|------|
| Broker unit tests | Go + mocked `RDSClientInterface` (no network) |
| Broker control-flow | `motoserver/moto` (free), Go SDK `BaseEndpoint` override |
| Fast SQL hardening | `gvenzl/oracle-free:23-slim-faststart` (native arm64) |
| RDS fidelity check | self-built `oracle/database:19.3.0-ee` (arm64) |
| Overlay runner | `cinc-auditor` + Oracle Instant Client sqlplus (or exec in container) |

## Alternatives considered

- **Treat local InSpec runs as evidence** — rejected: edition/privilege/host-control
  drift makes them unsound for accreditation.
- **Pay for LocalStack RDS** — rejected: still no Oracle engine; moto covers the
  control plane for free.
