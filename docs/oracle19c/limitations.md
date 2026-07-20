# Oracle 19c — known limitations & caveats

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

## Status

- **Dev/test only, not production-ready.** No live GovCloud RDS provisioning has
  been exercised yet (WS15 is gated on AWS credentials, cost, and license
  approval). Everything to date is validated by unit tests, a moto control-plane
  smoke, and a local Oracle SQL-hardening loop — **development signal, not
  compliance evidence** ([ADR-0005](../decisions/ADR-0005-local-testing-is-development-signal-only.md)).

## Credential model (intended boundary — not a limitation)

The broker returns the instance **master credential** per binding, the **same
model as the postgres/mysql RDS plans**. Creating least-privilege application
users *inside* the database is the **customer's** responsibility — the intended
shared-responsibility boundary (the platform does not reach into a customer's data
plane to mint DB principals). See [binding.md](binding.md) and the STIG guidance in
[hardening-baseline.md](hardening-baseline.md).

> **STIG note (Oracle-specific):** the Oracle RDS master user is more privileged
> than a postgres/mysql master (it carries the DBA-style role RDS grants). Bind
> apps to a **least-privilege user you create**, not to the master credential.
> This is customer guidance, not a broker-enforced gate. Broker-managed
> per-binding users are possible future hardening ([#534](https://github.com/cloud-gov/aws-broker/issues/534)),
> beyond the current documented boundary.

## Known limitations

1. **No in-place engine-version update** for Oracle yet
   (`oracle19cBaseline.SupportsEngineVersionUpdate()` returns false). Version is
   pinned by the plan; major-version handling for parameter/option groups is a
   follow-up.
2. **Fixed SID `ORCL`.** RDS Oracle constrains DBName/SID to ≤8 uppercase chars; the
   broker uses a stable `ORCL`. The unique per-instance identifier is the
   `DBInstanceIdentifier` (random `db<...>`), not the SID.
3. **Parameter/option/log support is unverified on live RDS GovCloud.** The
   hardened baseline is the STIG-recommended posture; each value must be confirmed
   supported+modifiable on the actual RDS Oracle 19c family before production
   (WS15). Unsupported values surface, not silently dropped.
4. **BYOL — customer-licensed, not broker-enforced (by design, not a gap).** The
   plan is `bring-your-own-license`; the customer is solely responsible for holding
   a valid Oracle license. cloud.gov does not manage, verify, or enforce it and
   does not gate provisioning on license evidence — we provide instructions
   ([licensing.md](licensing.md)) instead. This is an intentional responsibility
   boundary, not an unfinished feature.
5. **Option group empty.** No option-group options are enabled by default; some
   Oracle security features (e.g. native network encryption) may need options once
   GovCloud availability is confirmed ([#526](https://github.com/cloud-gov/aws-broker/issues/526)).
6. **OS/listener STIG controls are AWS-inherited / not applicable** on managed RDS
   (~33 controls). They are classified in the overlay's `control-layers.yml`, not
   remediated here. Any that cannot be met become POA&M candidates.
7. **A residual set of controls require manual review** (e.g. audit-log retention
   as a CloudWatch policy decision).
8. **`ENABLE_ORACLE` staged-rollout switch.** Oracle provisioning is off until an
   operator opts in. This is a rollout control for a new, not-yet-live-validated
   offering — **not** a security/boundary control (the credential model matches
   postgres/mysql).

## Not goals (by design)

- Not RDS Custom / not self-managed EC2 (managed RDS first, [ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)).
- Not a CSB brokerpak yet ([ADR-0001](../decisions/ADR-0001-implement-oracle-in-aws-broker-first.md); portability kept, [future-csb-migration.md](future-csb-migration.md)).
- The broker never runs InSpec/Cinc ([ADR-0002](../decisions/ADR-0002-keep-stig-validation-in-overlay-repo.md)).
