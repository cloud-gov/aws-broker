# Oracle 19c — licensing (SE2 + License Included)

The `oracle-se2-license-included-dev` plan uses Amazon RDS **License Included**: AWS
holds the Oracle Database license and bundles it into the instance price. Customers
do **not** buy or bring an Oracle license — no license key, no BYOL entitlement, no
attestation, no license parameter. There is **no unlicensed state**: cloud.gov never
facilitates an unlicensed Oracle database, because the license is held by AWS and is
inseparable from the running instance.

## Why License Included (and therefore SE2)

- **License Included is Standard Edition 2 (SE2) only** on RDS; Enterprise Edition
  is **BYOL-only**. The two are mutually exclusive — you cannot have License
  Included + EE. Choosing License Included to remove the unlicensed-Oracle liability
  means the offering is **SE2**.
- **Cost.** The bundled license makes an Oracle plan cost more than an
  open-source-engine plan. Cost is handled the same way as the Postgres/MySQL RDS
  plans — the instance carries `free: false` and the underlying AWS charge is
  passed through to agencies as **cloud.gov resource credits** (this broker's
  catalog does not itself carry a per-plan price figure).

## SE2 STIG deviations (ISSO risk/deviation)

SE2 does not include the Enterprise-Edition-only security features. The material
gaps, and how they are handled on this offering:

| EE-only feature (absent in SE2) | Handling on this offering |
|---|---|
| **Oracle-native TDE** (tablespace / column encryption) | **RDS storage-level encryption at rest (AES-256 / KMS)** (`encrypted: true`) — edition-independent, the same at-rest control the Postgres/MySQL plans use. |
| **Fine-Grained Auditing (FGA)** | **Standard / unified (mixed-mode) auditing** via `audit_trail` + CloudWatch `audit` export. (Even on EE, RDS does not export FGA events to CloudWatch.) |
| Oracle Database Vault | **Not applicable** — unsupported on RDS even for EE. |
| VPD, Label Security, Data Redaction, Partitioning, Data Guard | Not available on SE2; not part of this offering's control posture. |

Whether a specific STIG control is satisfied by **RDS-KMS at-rest encryption** in
place of Oracle-native TDE, and by **standard/unified auditing** in place of FGA, is
a control-interpretation decision recorded as an **ISSO risk/deviation acceptance**.
If a control is found to *require* Oracle-native TDE or FGA, the edition decision
(EE + BYOL, licensing handled by attestation) must be re-opened — see
[design-notes.md](design-notes.md).

## References

- [design-notes.md](design-notes.md) — SE2 + License Included rationale
- [README.md](README.md) — the `oracle-se2-license-included-dev` plan
- AWS: *Licensing Amazon RDS for Oracle* (License Included is SE2-only)
- AWS: *Amazon RDS for Oracle pricing* (License Included bundles the Oracle license)
