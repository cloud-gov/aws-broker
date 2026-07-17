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

`ssl_required` is `true`. Configure your driver to require/verify TLS to the RDS
endpoint (Oracle native network encryption / TLS is validated as an
inherited/option-group control by the overlay, not toggled by the app).
