# Oracle 19c — future migration to Cloud Service Broker (CSB)

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS18
> ([#533](https://github.com/cloud-gov/aws-broker/issues/533)).
> **Design note only — not implemented.** [ADR-0001](../decisions/ADR-0001-implement-oracle-in-aws-broker-first.md)
> chose `aws-broker` first; [ADR-0003](../decisions/ADR-0003-design-oracle-baseline-for-future-csb-portability.md)
> keeps the Oracle baseline portable so this migration stays viable.

## Why aws-broker first (recap)

The RDS provisioning lifecycle, credential encryption/storage, service bindings,
parameter/option-group machinery, and day-2 expectations already live in
`aws-broker`. Starting in [`cloud-gov/csb`](https://github.com/cloud-gov/csb) would
have created a second RDS operating model before any decision to move existing
Postgres/MySQL RDS there.

## What is portable (reuse in a brokerpak)

The hardened baseline is intentionally **data, not code**:

- [`services/rds/baselines/oracle19c/parameters.yml`](../../services/rds/baselines/oracle19c/parameters.yml)
  — parameter-group values map directly to a CSB brokerpak's Terraform
  `aws_db_parameter_group` `parameter {}` blocks.
- [`log_exports.yml`](../../services/rds/baselines/oracle19c/log_exports.yml) —
  `enabled_cloudwatch_logs_exports`.
- [`options.yml`](../../services/rds/baselines/oracle19c/options.yml) —
  `aws_db_option_group` options.
- The Oracle identifier rules (SID ≤8 upper; username constraints) and the binding
  shape (`oracle://` + `jdbc:oracle:thin` + `service_name` + `ssl_required`).
- The overlay is **already independent** of the broker
  ([ADR-0002](../decisions/ADR-0002-keep-stig-validation-in-overlay-repo.md)); its
  `control-layers.yml` and validation contract are broker-agnostic and need no
  change on migration.

## What is NOT portable (broker-specific behavior)

- The async **River job** create/modify lifecycle and `LastOperation` polling.
- Credential **encryption at rest** in the broker's own Postgres DB (gorm) and the
  `RDSCredentialUtils` scheme.
- The `RDSBaseline` Go interface itself (a brokerpak expresses the same decisions
  declaratively in service YAML + OpenTofu).
- The catalog `RDSPlan` struct / spruce templating.

## Migration concerns to resolve first

1. **Service-instance migration.** Moving existing instances from aws-broker to a
   CSB brokerpak means transferring OSBAPI instance/binding ownership without
   re-provisioning the RDS resource — needs a CSB "import existing resource" story
   and a state-import plan.
2. **Credential migration.** aws-broker stores encrypted master creds in its DB;
   CSB keeps per-instance OpenTofu state. Rotating/importing without downtime and
   without exposing master creds must be designed (ties into [#534](https://github.com/cloud-gov/aws-broker/issues/534)).
3. **Binding-contract compatibility.** The `VCAP_SERVICES` credential keys
   (`uri`, `jdbcUrl`, `service_name`, `ssl_required`, …) must be byte-for-byte
   preserved so bound apps do not break; a brokerpak `bind` template must emit the
   identical shape.
4. **Plan/GUID stability.** Service/plan GUIDs must be preserved (or aliased) so
   `cf` marketplace references and existing service instances remain valid.
5. **Parameter/option-group naming.** aws-broker uses the `cg-aws-broker-` prefix;
   a brokerpak would need to adopt or reconcile existing group names to avoid
   recreating groups (and rebooting instances).

## Open questions

- Does Cloud.gov intend to move **all** RDS brokerage to CSB, or Oracle-only?
- Who owns the one-time state import + credential re-encryption?
- Can the overlay validation contract be produced identically from CSB
  per-instance state (it should — it's just instance metadata)?

Until those are answered with an ADR, the brokerpak path stays documented-but-not-built.
