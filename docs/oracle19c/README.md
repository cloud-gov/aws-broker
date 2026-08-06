# Oracle Database 19c on Amazon RDS

Oracle Database **19c Standard Edition 2 (SE2)**, **License Included**, offered as
an Amazon RDS plan on the existing `aws-rds` service. It is provisioned, bound, and
rotated exactly like the other RDS plans (PostgreSQL / MySQL) — the generic
`cf create-service` / `cf bind-service` / `VCAP_SERVICES` flow is documented in the
top-level [README.md](../../README.md) and applies unchanged. This directory
documents **only** the Oracle-specific deltas.

## Plan

`oracle-se2-license-included-dev` — a **dev/test tier** plan (the `-dev` suffix
mirrors the sandbox-only `micro-psql` / `small-mysql` tier). **Not
production-ready.**

- `db.t3.medium`, 20 GB `gp3`, `encrypted: true` (KMS at rest)
- private only (no public accessibility)
- backup retention **14 days**
- `oracle-se2` 19c, `licenseModel: license-included`

## Oracle-specific deltas you must know

- **TLS on port 2484 (TCPS), not 1521.** RDS Oracle serves TLS on a dedicated TCPS
  listener. The binding advertises `port=2484`, `protocol=tcps`. Plaintext `1521`
  is the RDS default port. See [connection-examples.md](connection-examples.md).
- **Fixed SID `ORCL`.** RDS Oracle constrains DBName/SID to ≤8 uppercase chars; the
  broker uses a stable `ORCL`. The unique per-instance identifier is the
  `DBInstanceIdentifier`, not the SID.
- **Master credential per binding.** Same model as the other RDS engines — the
  binding returns the instance master credential; the customer creates
  least-privilege in-database users. This matters **more** on Oracle because the
  RDS master is **DBA-class**. See [limitations.md](limitations.md) and
  [../../ops/oracle19c/credential-rotation.md](../../ops/oracle19c/credential-rotation.md).
- **`ENABLE_ORACLE` staged-rollout flag.** Oracle provisioning is off unless the
  broker environment sets `ENABLE_ORACLE`. This gates when the new offering appears
  — it is **not** a security control.
- **License Included (no BYOL).** AWS holds the Oracle license and bundles it into
  the instance price — no license key, no attestation. See [licensing.md](licensing.md).

## Pages

- [licensing.md](licensing.md) — SE2 + License Included; SE2-vs-EE deltas
- [hardening-baseline.md](hardening-baseline.md) — hardened parameter/option/log baseline
- [connection-examples.md](connection-examples.md) — TCPS connect strings + client examples
- [limitations.md](limitations.md) — Oracle-specific limitations
- [design-notes.md](design-notes.md) — the durable "why" behind the design
- [../../ops/oracle19c/credential-rotation.md](../../ops/oracle19c/credential-rotation.md) — rotation
