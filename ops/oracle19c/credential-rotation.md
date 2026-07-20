# Oracle 19c — credential rotation

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

Oracle uses the **same credential + rotation model as the other RDS engines**
(postgres/mysql): the broker generates the master credential, stores it encrypted
at rest, and returns it in the binding. Rotation is **customer-initiated** via the
documented `rotate_credentials` parameter — there is no automatic/scheduled
rotation. This mirrors the public
[Rotate your credentials](https://docs.cloud.gov/platform/services/relational-database/#rotate-your-credentials)
documentation; the steps below are identical to the psql/mysql flow.

> **NOTE: Rotating your database credentials will likely incur some downtime for
> your application.** Depending on how your app handles connections, new
> connections may error until the app is re-bound and restaged with the new
> credential.

## Rotate

1. Rotate the master password:
   ```bash
   cf update-service my-oracle -c '{"rotate_credentials": true}'
   ```
2. Re-bind so the app receives the updated credential in `VCAP_SERVICES` (wait
   ~1 minute between unbind and bind, or the database may not be ready):
   ```bash
   cf unbind-service my-app my-oracle
   cf bind-service my-app my-oracle
   ```
   > The updated password is **not** pushed into a running app automatically —
   > `VCAP_SERVICES` is populated at bind time and frozen into the app environment,
   > so the app only sees the new credential after this re-bind + the restage below.
3. Delete and recreate any service keys to pick up the new credential:
   ```bash
   cf delete-service-key my-oracle my-key
   cf create-service-key my-oracle my-key
   ```
4. Restage (rolling to stay available):
   ```bash
   cf restage my-app --strategy rolling
   ```

## Notes

- Credentials are high-entropy random (`helpers.RandStrNoCaps(25)`), AES-encrypted
  with a per-instance salt, and stored by the broker — the same mechanism used for
  the psql/mysql plans.
- **Least privilege (Oracle-specific):** the rotated credential is the **instance
  master**, which on Oracle is a **DBA-class** account. Do **not** point production
  apps directly at the master. Connect as the master once to create a
  least-privilege application user, and bind/run your app as that user (see
  [binding.md](../../docs/oracle19c/binding.md)). Creating least-privilege
  in-database users is the customer's responsibility (the intended
  shared-responsibility boundary); rotating the master does not change an app user
  the customer created.
- Broker-managed per-binding users are possible future hardening
  ([#534](https://github.com/cloud-gov/aws-broker/issues/534)) — de-scoped, not a
  planned deliverable; do not rely on it.
