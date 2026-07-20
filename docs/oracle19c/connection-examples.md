# Oracle 19c — connecting a bound app

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

After `cf bind-service my-app my-oracle`, the binding credentials appear in
`VCAP_SERVICES` under the `aws-rds` service. The Oracle binding payload
(`services/rds/credentials.go`, [#528](https://github.com/cloud-gov/aws-broker/issues/528)):

```json
{
  "uri": "oracle://APP_USER:REDACTED@my-oracle.abc.us-gov-west-1.rds.amazonaws.com:1521/ORCL",
  "jdbcUrl": "jdbc:oracle:thin:@//my-oracle.abc.us-gov-west-1.rds.amazonaws.com:1521/ORCL",
  "username": "APP_USER",
  "password": "REDACTED",
  "host": "my-oracle.abc.us-gov-west-1.rds.amazonaws.com",
  "port": "1521",
  "service_name": "ORCL",
  "sid": "ORCL",
  "db_name": "ORCL",
  "name": "ORCL",
  "ssl_required": "true"
}
```

> **Security note ([#534](https://github.com/cloud-gov/aws-broker/issues/534)):**
> the binding currently returns the instance master credential (parity with the
> other RDS engines). A per-binding least-privilege application user is a tracked
> follow-up. The payload never advertises admin/master markers, and a test asserts
> this (`TestOracleBindingDoesNotLeakAdminMarkers`).

## Reading credentials

```bash
cf env my-app   # inspect VCAP_SERVICES.aws-rds[0].credentials
```

## Space egress (first-time setup)

By default every Cloud.gov space is **closed-egress** — an app cannot reach its
brokered database until you open egress. This is a self-service step (no operator
needed), same as the documented psql/mysql flow:

```bash
cf bind-security-group trusted_local_networks_egress <ORG> --space <SPACE>
```

Without it you'll see `connection refused` / connection-timeout errors.

## Connecting from your laptop (SSH tunnel)

Cloud.gov databases are **not reachable directly from your machine** — connect
through an SSH tunnel (same as psql/mysql):

```bash
# open a tunnel (keep it running):
cf ssh -N -L 1521:<host>:1521 my-app
# then use localhost:1521 with SQLcl / sqlplus / your client, e.g.:
sql APP_USER/"$PASSWORD"@//localhost:1521/ORCL
```

`<host>`, `username`, `password` come from `cf env my-app`.

## JDBC (Java)

```java
String url = System.getenv("ORACLE_JDBC_URL"); // wire jdbcUrl into your app config
// jdbc:oracle:thin:@//host:1521/ORCL
Properties p = new Properties();
p.put("user", username);
p.put("password", password);
p.put("oracle.net.ssl_server_dn_match", "true"); // ssl_required=true
Connection c = DriverManager.getConnection(url, p);
```

## SQLcl / SQL*Plus

```bash
# EZConnect using the service name from the binding:
sql APP_USER/"$PASSWORD"@//host:1521/ORCL
# or
sqlplus APP_USER/"$PASSWORD"@//host:1521/ORCL
```

## Python (python-oracledb, thin mode)

```python
import oracledb
conn = oracledb.connect(user=username, password=password,
                        dsn=f"{host}:1521/ORCL")
```

## TLS

`ssl_required` is `true` — configure your driver to require/verify TLS to the RDS
endpoint. TLS to the RDS endpoint is an AWS/RDS-inherited concern the overlay is
*designed to* validate (pending live validation, WS15); option-group-based Oracle
network-encryption options are a follow-up ([#526](https://github.com/cloud-gov/aws-broker/issues/526))
and are **not** provisioned yet.

> **Use a least-privilege user, not the master.** The bound credential is the
> instance master, which on Oracle is **DBA-class**. Connect as the master once to
> create a least-privilege application user, then run your app as that user — see
> [binding.md](binding.md). Creating in-database users is the customer's
> responsibility.
