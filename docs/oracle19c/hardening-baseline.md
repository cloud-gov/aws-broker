# Oracle 19c — hardening baseline

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.
> The authoritative values live in the reviewable, embedded baseline files under
> [`services/rds/baselines/oracle19c/`](../../services/rds/baselines/oracle19c/).

Oracle is **born hardened** ([ADR-0003](../decisions/ADR-0003-design-oracle-baseline-for-future-csb-portability.md)):
unlike the MySQL/Postgres opt-in pattern, every brokered Oracle instance is
provisioned with a broker-managed hardened parameter group and default audit log
exports.

## Parameter group (`baselines/oracle19c/parameters.yml`)

| Parameter | Value | Apply | STIG intent |
|-----------|-------|-------|-------------|
| `audit_trail` | `DB,EXTENDED` | pending-reboot | enable DB auditing |
| `audit_sys_operations` | `TRUE` | pending-reboot | audit privileged SYS ops |
| `sec_case_sensitive_logon` | `TRUE` | immediate | case-sensitive passwords |
| `remote_login_passwordfile` | `NONE` | pending-reboot | disable remote OS password-file auth |
| `resource_limit` | `TRUE` | immediate | enforce profile resource limits |
| `sql92_security` | `TRUE` | pending-reboot | SQL92 DML-predicate least privilege |

`pending-reboot` parameters take effect after a reboot; the broker surfaces
pending-reboot state via the existing async/modify path.

> **SE2 applicability.** Every parameter in this baseline (`audit_trail`,
> `audit_sys_operations`, `sec_case_sensitive_logon`, `remote_login_passwordfile`,
> `resource_limit`, `sql92_security`) is a **base Oracle init parameter available in
> Standard Edition 2 (SE2)** — none are Enterprise-Edition-only — so the
> born-hardened parameter baseline **fully applies** on the SE2 + License Included
> offering ([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)).
>
> **Not part of the SE2 posture (EE-only):** Oracle-native **Transparent Data
> Encryption (TDE)** and **Fine-Grained Auditing (FGA)** are Enterprise-Edition
> features and are **not** available on SE2. At-rest encryption is instead provided
> by **RDS storage-level encryption (AES-256 / KMS)** (edition-independent, `encrypted:
> true`), and auditing by **standard / unified auditing** via `audit_trail` +
> CloudWatch `audit` export. These SE2 deviations are documented and ISSO-accepted —
> see [licensing.md](licensing.md).

## Log exports (`baselines/oracle19c/log_exports.yml`)

Default: `alert`, `audit`, `listener`. `trace` and `oemagent` are opt-in
(high-volume / requires OEM).

## Option group (`baselines/oracle19c/options.yml`)

The SSL option is provisioned and attached at create; **no attack-surface options**
(Oracle XML DB HTTP listener, external procedures / `extproc`, Java VM, APEX) are
enabled ([#535](https://github.com/cloud-gov/aws-broker/issues/535)). A test asserts
this.

### Encryption in transit — SSL option (SC-8 / SC-8(1) / SC-13)

The create path provisions an RDS Oracle **SSL option group** and attaches it,
serving TLS on a dedicated **TCPS listener (port 2484)**:

| Setting | Value | Intent |
|---------|-------|--------|
| listener port | `2484` (TCPS) | separate TLS listener (RDS Oracle serves TLS off-port) |
| `SQLNET.SSL_VERSION` | `1.2` | TLS 1.2 — RDS Oracle ceiling (no 1.3); acceptable for FedRAMP Moderate |
| `SQLNET.CIPHER_SUITE` | `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384` | FedRAMP + FIPS AEAD suite; RSA-CA-compatible |
| `FIPS.SSLFIPS_140` | `TRUE` | FIPS 140 crypto mode (STIG V-270571) |

This delivers **encryption-in-transit** (SC-8 / SC-8(1)) with FIPS-validated crypto
(SC-13); DISA Oracle 19c STIG V-270579 (SC-8(2)) + V-270571 (SC-13). The instance
uses the RDS default RSA CA (`rds-ca-rsa2048-g1`), compatible with the ECDHE_RSA
cipher (an ECDSA-only suite would need the ECC CA and is rejected fail-closed before
the AWS call). `FIPS.SSLFIPS_140=TRUE` means clients **must** negotiate a
FIPS/FedRAMP cipher or the handshake fails. The binding advertises the TCPS/2484
posture (`port=2484`, `protocol=tcps`, `ssl_required=true`, `ssl_server_dn_match`,
`ca_cert_bundle_url`); the client verifies the server cert against the CA bundle
(no hardcoded DN is published).

> **TLS-only posture depends on the platform SG ([#541](https://github.com/cloud-gov/aws-broker/issues/541)).**
> The broker provisions + attaches the SSL option and expresses the 2484 intent, but
> **cannot** open `2484` ingress or deny plaintext `1521` — that is a platform
> security-group change (cg-provision) outside the broker's IAM. Until #541 lands,
> `1521` plaintext stays reachable and the plan is **not customer-ready**. Verified
> offline + go unit tests (`TestOracleSSLOptionBaseline`,
> `TestOracleBaselineOptionsBuild`, and `TestGetCredentials` oracle-se2 asserting the
> TCPS payload); not validated against live GovCloud RDS (WS15).

## SQL-level hardening (overlay, not the broker)

Profiles, default-account lockout, unified audit policies, and detect-first
PUBLIC-grant review are applied by the SQL layer in
[`cg-oracle-database-19c-stig-overlay/hardening/sql/`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/tree/main/hardening/sql)
([ADR-0002](../decisions/ADR-0002-keep-stig-validation-in-overlay-repo.md)).
These are assessment-first, idempotent, and written for the RDS master-user model
(no `SYS`). Verified locally against Oracle Free (arm64) as a non-SYS user:
FAILED_LOGIN_ATTEMPTS 3 / PASSWORD_LIFE_TIME 35 / INACTIVE_ACCOUNT_TIME 35 and 2
unified audit policies enabled, idempotent on re-run — **development signal only**
([ADR-0005](../decisions/ADR-0005-local-testing-is-development-signal-only.md)).

## Control layering

Not every STIG control is a SQL check on RDS. The overlay's
[`control-layers.yml`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/blob/main/control-layers.yml)
classifies each control: `broker_infra`, `aws_rds_parameter_group`,
`aws_rds_option_group`, `sql_hardening`, `sql_assessment_only`, `aws_inherited`,
`not_applicable_rds`, `manual_review`, `compensating_control`, `blocked`. This is
what stops the ~33 OS/listener controls from failing misleadingly on managed RDS.

## Verification obligation

Every parameter/option/log value **must be verified as supported+modifiable on the
live RDS Oracle 19c engine in GovCloud** before a production deploy (WS15, gated).
Unsupported values are surfaced, never silently dropped.
