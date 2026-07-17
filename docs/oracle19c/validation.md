# Oracle 19c — running STIG validation

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.
> The broker never runs InSpec/Cinc ([ADR-0002](../decisions/ADR-0002-keep-stig-validation-in-overlay-repo.md)).

Validation is performed by [`cg-oracle-database-19c-stig-overlay`](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay)
(CINC/InSpec, `oracledb_session` via `sqlplus`) against a brokered Oracle instance,
fed a broker-produced **validation contract**.

## Flow

1. Provision + bind an Oracle service; obtain an assessment DB credential
   (out-of-band via the credential *ref*, never in the contract).
2. Render the validation contract
   ([schema](validation-contract.schema.json) / [doc](validation-contract.md)) from
   the instance metadata. It contains endpoint/engine/parameter-group/option-group/
   log-exports/`environment` and credential **refs only** — **no secrets**.
3. Open the tunnel to the private RDS endpoint (`cf ssh` + `cf connect-to-service`),
   render the InSpec `--input-file` (`user`, `password`, `host`, `port`, `service`,
   `sqlplus_bin` + org allow-lists), and run `cinc-auditor exec`.
4. Apply the overlay's `control-layers.yml` so `aws_rds_parameter_group`,
   `aws_inherited`, and `not_applicable_rds` controls report correctly on managed
   RDS instead of failing.
5. `environment` in the contract distinguishes `local-dev` (development signal
   only) from real RDS evidence.

## SQL-layer hardening (before validation)

The overlay's `hardening/sql/` scripts (assessment-first, idempotent, non-SYS)
bring the SQL layer to the STIG posture. Verified locally on Oracle Free (arm64):
profile limits (3 / 35 / 35) + 2 unified audit policies, idempotent — development
signal only ([ADR-0005](../decisions/ADR-0005-local-testing-is-development-signal-only.md)).

## Evidence

The InSpec JSON reporter output is the evidence artifact (viewable in
Heimdall-Lite). Findings are triaged into the layers from `control-layers.yml`;
anything that cannot be met on RDS becomes a POA&M candidate. **Authoritative
evidence requires the gated live GovCloud run (WS15).**
