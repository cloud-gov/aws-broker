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
   exports (`alert`/`audit`/`listener`), and attaches the **born-hardened**
   parameter group.
5. On ready, the instance is marked available; bind is synchronous.

Verified against a **moto** RDS control-plane mock: `create-db-instance` (oracle-se2
19c, encrypted, License Included, private), `create-db-parameter-group` (`oracle-se2-19`), and
`create-option-group` (oracle-se2 19) are all accepted — control-flow signal, not
compliance evidence ([ADR-0005](../decisions/ADR-0005-local-testing-is-development-signal-only.md)).

## Deprovision

`cf delete-service my-oracle` runs the delete worker; the broker's custom
parameter/option groups are cleaned up by the existing prefix-sweep
(`CleanupCustomParameterGroups` / `CleanupCustomOptionGroups`).
