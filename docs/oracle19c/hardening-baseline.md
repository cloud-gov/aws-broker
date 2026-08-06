# Oracle 19c — hardening baseline

Every brokered Oracle instance is provisioned with a broker-managed hardened
parameter group, default audit log exports, and an SSL option group. The
authoritative values live in the reviewable, embedded baseline files under
[`services/rds/baselines/oracle19c/`](../../services/rds/baselines/oracle19c/); this
page documents the durable deltas and the controls they map to.

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
pending-reboot state via the existing async/modify path. All six are base Oracle
init parameters available in SE2 (none are EE-only), so the baseline fully applies.

## Log exports (`baselines/oracle19c/log_exports.yml`)

Default CloudWatch exports: `alert`, `audit`, `listener`. `trace` and `oemagent` are
opt-in (high-volume / requires OEM).

## Option group — SSL / TCPS (SC-8 / SC-8(1) / SC-13)

The create path provisions an RDS Oracle **SSL option group** and attaches it,
serving TLS on a dedicated **TCPS listener (port 2484)**:

| Setting | Value | Intent |
|---------|-------|--------|
| listener port | `2484` (TCPS) | separate TLS listener (RDS Oracle serves TLS off-port) |
| `SQLNET.SSL_VERSION` | `1.2` | TLS 1.2 — RDS Oracle ceiling (no 1.3); acceptable for FedRAMP Moderate |
| `SQLNET.CIPHER_SUITE` | `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384` | FIPS AEAD suite; RSA-CA-compatible |
| `FIPS.SSLFIPS_140` | `TRUE` | FIPS 140 crypto mode |

This delivers encryption-in-transit (SC-8 / SC-8(1)) with FIPS-validated crypto
(SC-13); DISA Oracle 19c STIG **V-270579** (SC-8(2)) + **V-270571** (SC-13). The
instance uses the RDS default RSA CA (`rds-ca-rsa2048-g1`), compatible with the
ECDHE_RSA cipher (an ECDSA-only suite would need the ECC CA and is rejected
fail-closed before the AWS call). `FIPS.SSLFIPS_140=TRUE` means clients **must**
negotiate a FIPS cipher or the handshake fails. No attack-surface options (XML DB
HTTP listener, `extproc`, Java VM, APEX) are enabled.

> **TLS-only depends on the platform security group (durable operational fact).**
> The broker provisions + attaches the SSL option and expresses the 2484 intent, but
> **cannot** open `2484` ingress or deny plaintext `1521` — that is a platform
> security-group change owned by cg-provision, outside the broker's IAM. TLS-only
> enforcement requires cg-provision to allow `2484` and deny `1521`.

## STIG validation lives in the overlay

SQL-level hardening (profiles, default-account lockout, unified audit policies,
PUBLIC-grant review) and full STIG validation are **not** in the broker. They live
in the separate
[`cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay)
repo, whose `control-layers.yml` classifies each control by layer
(`broker_infra`, `aws_rds_parameter_group`, `aws_rds_option_group`, `sql_hardening`,
`aws_inherited`, `not_applicable_rds`, …) — which is what stops the ~33 OS/listener
controls from failing misleadingly on managed RDS. The broker configures; the
overlay validates. See [design-notes.md](design-notes.md).
