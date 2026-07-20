# Oracle 19c — provisioning

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

`cf create-service aws-rds oracle-se2-license-included-dev my-oracle` drives the standard RDS
async lifecycle (see [current-state-analysis.md](current-state-analysis.md) §2):

1. `rdsBroker.CreateInstance` validates options, fetches the `oracle-se2-license-included-dev`
   plan, and builds an `RDSInstance`.
2. **Fail-closed identifier validation** runs before any AWS call
   ([#524](https://github.com/cloud-gov/aws-broker/issues/524)): Oracle SID (`ORCL`,
   ≤8 upper) and master username (8–30 chars, non-reserved).
3. A **River job** is enqueued; `CreateInstance` returns `in progress`. CF polls
   `LastOperation`.
4. The worker builds `CreateDBInstanceInput`: `Engine=oracle-se2`, pinned
   `EngineVersion`, `LicenseModel=license-included`, `StorageEncrypted`,
   private (no public accessibility), backup retention, default CloudWatch log
   exports (`alert`/`audit`/`listener`), and attaches the **born-hardened
   parameter group**. (No option group is provisioned yet — the Oracle option
   baseline is empty; tracked in [#526](https://github.com/cloud-gov/aws-broker/issues/526).)
5. On ready, the instance is marked available; bind is synchronous.

Exercised against a **moto** RDS control-plane mock (control-flow signal, **not**
compliance evidence — [ADR-0005](../decisions/ADR-0005-local-testing-is-development-signal-only.md)):
the `create-db-instance` (oracle-se2 19c, encrypted, License Included, private) and
`create-db-parameter-group` (`oracle-se2-19`) calls are accepted by the AWS API.
This confirms the broker's request *shape*, not real RDS behavior; no live GovCloud
provisioning has run (WS15).

## Deprovision

`cf delete-service my-oracle` runs the delete worker; the broker's custom
parameter/option groups are cleaned up by the existing prefix-sweep
(`CleanupCustomParameterGroups` / `CleanupCustomOptionGroups`).
