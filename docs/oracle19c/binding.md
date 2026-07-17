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

## Credential model (current + planned)

- **Current:** the binding returns the instance **master credential** (parity with
  existing RDS engines). Encrypted at rest in the broker DB.
- **Planned ([#534](https://github.com/cloud-gov/aws-broker/issues/534)):** a
  per-binding least-privilege Oracle application user so apps never receive master
  creds; unbind drops the binding user.

## Unbind & rotation

`cf unbind-service` removes the binding. To rotate: re-provision / update and
re-bind, then `cf restage --strategy rolling`. A stale credential is denied
(fail-closed). See [ops/oracle19c/credential-rotation.md](../../ops/oracle19c/credential-rotation.md).
