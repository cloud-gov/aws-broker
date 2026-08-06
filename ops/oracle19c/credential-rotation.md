# Oracle 19c — credential rotation

Oracle uses the **same customer-initiated rotation flow as the other RDS engines**
(Postgres/MySQL): `cf update-service -c '{"rotate_credentials": true}'`, then
unbind/bind and restage. The full step-by-step (including the downtime caveat and
the need to recreate service keys) is documented for the platform at
[docs.cloud.gov — Rotate your credentials](https://docs.cloud.gov/platform/services/relational-database/#rotate-your-credentials);
the Oracle steps are identical. There is no automatic/scheduled rotation.

```bash
cf update-service my-oracle -c '{"rotate_credentials": true}'
cf unbind-service my-app my-oracle
cf bind-service my-app my-oracle       # VCAP_SERVICES is populated at bind time
cf restage my-app --strategy rolling
```

## Oracle-specific note: the master is DBA-class

The rotated credential is the **instance master**, which on Oracle is a **DBA-class**
account — more privileged than a Postgres/MySQL master. Do **not** point production
apps directly at the master. Connect as the master once to create a least-privilege
application user, and bind/run your app as that user; rotating the master does not
change an app user the customer created. Creating least-privilege in-database users
is the customer's responsibility (the intended shared-responsibility boundary). See
[../../docs/oracle19c/limitations.md](../../docs/oracle19c/limitations.md).
