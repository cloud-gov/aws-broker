# Oracle 19c — binding

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

`cf bind-service my-app my-oracle` is **synchronous**. The broker resolves the live
RDS endpoint (host/port), decrypts the stored password, and returns the Oracle
binding payload into `VCAP_SERVICES` (keys documented in
[connection-examples.md](connection-examples.md)).

## What the app receives

`uri` (oracle scheme), `jdbcUrl` (thin), `username`, `password`, `host`, `port`
(1521), `service_name`/`sid` (`ORCL`), `ssl_required=true`. No admin/master marker
keys are present (asserted by `TestOracleBindingDoesNotLeakAdminMarkers`).

## Credential model (intended shared-responsibility boundary)

The binding returns the instance **master credential** — the **same model as the
postgres/mysql RDS plans**. Creating least-privilege application users inside the
database is the **customer's** responsibility; the platform does not reach into the
customer's data plane to create DB principals. Credentials are encrypted at rest in
the broker DB.

> **STIG note:** the Oracle master user is privileged (DBA-style). Do **not** point
> production apps at the master credential — connect as the master once, create a
> least-privilege schema/user for your app, and use that. Example:
>
> ```sql
> -- as the master user (from the binding), one time:
> CREATE USER app_ro IDENTIFIED BY "<strong-pw>";
> GRANT CREATE SESSION TO app_ro;
> GRANT SELECT ON your_schema.your_table TO app_ro;
> ```
>
> Broker-managed per-binding users are possible future hardening
> ([#534](https://github.com/cloud-gov/aws-broker/issues/534)) — beyond the current
> documented boundary, not a gap.

## Unbind & rotation

`cf unbind-service` removes the binding. To rotate: re-provision / update and
re-bind, then `cf restage --strategy rolling`. A stale credential is denied
(fail-closed). See [ops/oracle19c/credential-rotation.md](../../ops/oracle19c/credential-rotation.md).
