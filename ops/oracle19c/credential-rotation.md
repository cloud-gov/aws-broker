# Oracle 19c — operator: credential rotation

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

The broker stores the Oracle master credential encrypted at rest and returns it in
bindings (current model, [#534](https://github.com/cloud-gov/aws-broker/issues/534)).

## Rotate

Self-service (matches the documented psql/mysql flow — no operator needed):

1. Rotate the master password:
   ```bash
   cf update-service my-oracle -c '{"rotate_credentials": true}'
   ```
2. Re-bind so the app receives the new credential (wait ~1 min between unbind/bind):
   ```bash
   cf unbind-service my-app my-oracle
   cf bind-service my-app my-oracle
   ```
3. Recreate any service keys:
   ```bash
   cf delete-service-key my-oracle my-key
   cf create-service-key my-oracle my-key
   ```
4. Restage (rolling to stay available):
   ```bash
   cf restage my-app --strategy rolling
   ```

> Rotating credentials can incur brief downtime depending on how the app pools
> connections — same caveat cloud.gov documents for psql/mysql.

## Notes

- Credentials are high-entropy random (`helpers.RandStrNoCaps(25)`), AES-encrypted
  with a per-instance salt.
- The proxy/DB logs failed auth (AU-2); review via CloudWatch `audit`/`listener`
  exports.
- The planned per-binding least-privilege user ([#534](https://github.com/cloud-gov/aws-broker/issues/534))
  will make rotation binding-scoped.
