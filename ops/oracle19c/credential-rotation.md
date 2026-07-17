# Oracle 19c — operator: credential rotation

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

The broker stores the Oracle master credential encrypted at rest and returns it in
bindings (current model, [#534](https://github.com/cloud-gov/aws-broker/issues/534)).

## Rotate

1. Re-provision or `cf update-service` to regenerate the credential (broker
   generates a new random password, re-encrypts, updates RDS master password).
2. Re-bind affected apps: `cf unbind-service` → `cf bind-service` →
   `cf restage --strategy rolling`.
3. A stale credential is denied (fail-closed) until the app re-reads
   `VCAP_SERVICES`.

## Notes

- Credentials are high-entropy random (`helpers.RandStrNoCaps(25)`), AES-encrypted
  with a per-instance salt.
- The proxy/DB logs failed auth (AU-2); review via CloudWatch `audit`/`listener`
  exports.
- The planned per-binding least-privilege user ([#534](https://github.com/cloud-gov/aws-broker/issues/534))
  will make rotation binding-scoped.
