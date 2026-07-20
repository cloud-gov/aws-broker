# Local Oracle 19c hardening + assessment harness

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS10/11
> ([#529](https://github.com/cloud-gov/aws-broker/issues/529)).
>
> **⚠️ DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE ([ADR-0005](../docs/decisions/ADR-0005-local-testing-is-development-signal-only.md)).**
> Everything produced here (SQL assessment/hardening diffs, before/after reports)
> is for fast iteration. Authoritative STIG evidence comes only from running the
> `cg-oracle-database-19c-stig-overlay` against a real brokered GovCloud RDS Oracle
> instance (the gated dev proof, WS15).

## What this is

Two independent local layers so you can iterate without AWS:

1. **`moto`** (free) — mocks the AWS **RDS control plane** so the broker's Go
   create/parameter-group/option-group flow can be exercised end-to-end without
   real AWS. It does **not** run an Oracle engine (no local tool does for RDS).
2. **`gvenzl/oracle-free`** (native arm64) — a real Oracle engine to develop and
   idempotency-test the SQL hardening scripts against, and to run the overlay's
   `oracledb_session` controls. Edition/version/privilege differ from the brokered
   RDS engine (Oracle SE2 19c); the local self-built 19c-EE image is a stricter
   upper bound. So this is signal, not proof.

## Caveats (what local CANNOT tell you)

- RDS parameter-group / option-group **effects** (only that the broker issues the
  right API calls, via moto).
- KMS encryption, CloudWatch log exports, GovCloud networking/partition.
- The RDS master-user **privilege model** — RDS does not grant `SYS`/`SYSDBA`.
  Develop hardening as a **non-SYS** privileged user (this harness seeds one) so
  RDS-only permission failures surface early.
- Oracle **edition/version** drift: `oracle-free` is 23c Free, not 19c EE. Use the
  self-built 19c-EE image (see below) for a fidelity pass.

## Prerequisites

- Docker (running).
- For the overlay: `cinc-auditor` + Oracle Instant Client `sqlplus`, OR run the
  overlay's own container. See the overlay repo.

## Usage

```bash
# Broker control-flow against a mock RDS control plane (no Oracle engine):
make -C local moto-up          # start moto on :5000
# point the broker/tests at http://localhost:5000 (BaseEndpoint override)

# Real local Oracle for SQL hardening iteration:
make -C local oracle-up        # start gvenzl/oracle-free, wait for healthy
make -C local seed             # create non-SYS app user + seed insecure state
make -C local assess           # run assessment SQL -> local/reports/ (labeled)
make -C local harden           # apply allowed hardening (idempotent)
make -C local assess           # re-assess: state should have changed
make -C local down             # tear everything down
```

## Layout

```
local/
  README.md                     (this file)
  Makefile
  docker-compose.oracle-free.yml   gvenzl/oracle-free (native arm64)
  docker-compose.oracle-19c.yml    self-built oracle/database:19.3.0-ee (fidelity)
  docker-compose.moto.yml          motoserver/moto (free RDS control-plane mock)
  scripts/
    wait-for-oracle.sh
    run-assessment-local.sh
    run-hardening-local.sh
  init/
    00_create_test_users.sql       non-SYS privileged app user (mirrors RDS)
    01_seed_insecure_state.sql     deliberately-weak state for detection tests
  reports/                         (gitignored) assessment/hardening output
```

The **authoritative SQL hardening scripts** live in the overlay repo
(`hardening/sql/`, per [ADR-0002](../docs/decisions/ADR-0002-keep-stig-validation-in-overlay-repo.md));
this harness invokes copies/symlinks for a fast local loop. It must not be wired
into the broker runtime.
