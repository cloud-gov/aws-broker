# Oracle 19c — known limitations & caveats

Oracle-specific limitations only. Generic RDS behavior is documented in the
top-level [README.md](../../README.md).

## Credential model (intended boundary — not a limitation)

The binding returns the instance **master credential**, the same model as the
Postgres/MySQL RDS plans. Creating least-privilege application users *inside* the
database is the customer's responsibility — the intended shared-responsibility
boundary. On Oracle this matters more: the RDS master user is **DBA-class** (it
carries the DBA-style role RDS grants), more privileged than a Postgres/MySQL
master. Bind apps to a **least-privilege user you create**, not to the master. This
is customer guidance, not a broker-enforced gate.

## Limitations

1. **No in-place engine-version update.** `oracle19cBaseline.SupportsEngineVersionUpdate()`
   returns false; version is pinned by the plan.
2. **Fixed SID `ORCL`.** RDS Oracle constrains DBName/SID to ≤8 uppercase chars. The
   unique per-instance identifier is the `DBInstanceIdentifier`, not the SID.
3. **SE2 lacks the EE-only features.** No Oracle-native TDE, Fine-Grained Auditing
   (FGA), VPD, Label Security, Data Redaction, Partitioning, or Data Guard. At-rest
   encryption is provided by RDS-KMS storage encryption and auditing by
   standard/unified auditing; the residual TDE/FGA control interpretation is an
   accepted ISSO risk/deviation — see [licensing.md](licensing.md). An accepted
   deviation of the chosen edition, not an unfinished feature.
4. **TLS-only needs the platform security group.** The broker provisions + attaches
   the SSL option group (TCPS 2484, TLS 1.2, `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`,
   `FIPS.SSLFIPS_140=TRUE`) and the binding advertises TCPS/2484, but it cannot open
   `2484` ingress or deny plaintext `1521` — that is a cg-provision security-group
   change. See [hardening-baseline.md](hardening-baseline.md).
5. **TLS 1.2 ceiling.** RDS Oracle SSL supports TLS 1.2 only (no 1.3). Acceptable for
   FedRAMP Moderate.
6. **~33 OS/listener STIG controls are AWS-inherited / not applicable** on managed
   RDS. They are classified in the overlay's `control-layers.yml`, not remediated in
   the broker; any that cannot be met become POA&M candidates.
7. **`ENABLE_ORACLE` staged-rollout switch.** Oracle provisioning is off until the
   broker environment opts in. A rollout control for a new offering — **not** a
   security/boundary control (the credential model matches Postgres/MySQL).
