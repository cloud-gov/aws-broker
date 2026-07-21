# Oracle 19c — design notes

The durable rationale behind the Oracle offering. The code and tests are the source
of truth; this captures the non-obvious "why".

## SE2 + License Included

License Included is **SE2-only** on RDS; Enterprise Edition is **BYOL-only** — they
are mutually exclusive. License Included is chosen to remove the unlicensed-Oracle
liability: AWS holds the license and it is inseparable from the running instance, so
cloud.gov cannot facilitate an unlicensed Oracle DB on GSA-operated infrastructure
(BYOL would only reallocate that liability via attestation, not remove it). The
consequence is SE2, which lacks the EE-only features (notably Oracle-native TDE and
FGA). Those SE2 STIG deltas are accepted as an ISSO risk/deviation, with RDS-KMS
at-rest encryption and standard/unified auditing as the compensating controls. If a
control is ever found to *require* TDE or FGA, the edition decision must be re-opened
(EE + BYOL). See [licensing.md](licensing.md).

## STIG validation lives in the overlay, not the broker

The broker is a Go service that provisions and configures RDS. STIG validation
(InSpec/Cinc controls, SQL assessment, RDS-applicability mapping, evidence) lives in
the separate `cg-oracle-database-19c-stig-overlay` repo. **The broker never runs
InSpec/Cinc.** This keeps a clean separation of duties — the thing being audited does
not audit itself — avoids a Ruby/InSpec runtime in a Go broker, and lets controls
evolve independently. **Broker configures; overlay validates.** See
[hardening-baseline.md](hardening-baseline.md).

## Hardened baseline as embedded YAML behind a per-engine abstraction

Engine behavior is dispatched through an `RDSBaseline` engine-strategy interface
(`mysqlBaseline`, `postgresBaseline`, `oracle19cBaseline`), and the hardened values
live as structured YAML under `services/rds/baselines/oracle19c/` rather than
hard-coded Go conditionals. This makes the hardening values directly reviewable by a
security reviewer, keeps Oracle logic out of scattered `if DbType == "..."`
conditionals, and keeps the declarative baseline portable if RDS brokerage ever
moves to CSB (which is also declarative). Broker *runtime* behavior (async
lifecycle, credential encryption, state) is not portable and is deliberately out of
the baseline files.

## Why `ENABLE_ORACLE` exists

`ENABLE_ORACLE` gates a staged rollout of a new offering not yet validated on a live
foundation — it governs when the offering appears, not per-request approval. It is
**not** a security or boundary control: Oracle uses the same credential model as the
Postgres/MySQL plans (master credential per binding; customer creates least-privilege
users).
