# Oracle 19c — connecting a bound app

After `cf bind-service`, the credentials appear in `VCAP_SERVICES` under the
`aws-rds` service. Reading credentials (`cf env`), opening space egress
(`cf bind-security-group trusted_local_networks_egress`), and tunnelling from a
laptop (`cf ssh -L`) are identical to the Postgres/MySQL flow — see the top-level
[README.md](../../README.md). This page covers only the Oracle-specific connection
detail.

## Binding payload (Oracle-specific keys)

```json
{
  "uri": "oracle://APP_USER:REDACTED@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=my-oracle.abc.us-gov-west-1.rds.amazonaws.com)(PORT=2484))(CONNECT_DATA=(SID=ORCL)))",
  "jdbcUrl": "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=my-oracle.abc.us-gov-west-1.rds.amazonaws.com)(PORT=2484))(CONNECT_DATA=(SID=ORCL)))",
  "port": "2484",
  "protocol": "tcps",
  "service_name": "ORCL",
  "sid": "ORCL",
  "ssl_required": "true",
  "ssl_server_dn_match": "true",
  "ca_cert_bundle_url": "https://truststore.pki.us-gov-west-1.rds.amazonaws.com/global/global-bundle.pem"
}
```

The binding uses a full Oracle connect `DESCRIPTION` on the **TCPS listener (port
2484)** — not EZConnect, which cannot express `PROTOCOL=TCPS`. `ssl_required=true`
and `ssl_server_dn_match=true` tell the client to encrypt **and** verify the server
identity against `ca_cert_bundle_url` (the GovCloud RDS CA bundle). No hardcoded
server-cert DN is published — the RDS cert subject is Amazon-owned and rotates, so
the driver derives it from the trusted cert. The instance uses the RDS default RSA CA
(`rds-ca-rsa2048-g1`), compatible with the ECDHE_RSA cipher.

## JDBC (Java)

```bash
# import the GovCloud RDS CA bundle (ca_cert_bundle_url from the binding):
wget https://truststore.pki.us-gov-west-1.rds.amazonaws.com/global/global-bundle.pem
keytool -importcert -alias rds-ca -file global-bundle.pem \
  -keystore truststore.jks -storepass "$TRUSTSTORE_PW" -noprompt
```

```java
String url = System.getenv("ORACLE_JDBC_URL"); // the TCPS jdbcUrl from the binding
Properties p = new Properties();
p.put("user", username);
p.put("password", password);
p.put("oracle.net.ssl_server_dn_match", "true"); // ssl_server_dn_match=true
p.put("javax.net.ssl.trustStore", "truststore.jks");
p.put("javax.net.ssl.trustStorePassword", System.getenv("TRUSTSTORE_PW"));
Connection c = DriverManager.getConnection(url, p);
```

## SQL*Plus

```bash
# full TCPS connect descriptor (EZConnect cannot express PROTOCOL=TCPS):
sqlplus 'APP_USER@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=<host>)(PORT=2484))(CONNECT_DATA=(SID=ORCL)))'
```

Configure a wallet / truststore holding the imported GovCloud RDS CA bundle so the
client trusts the server certificate.

## Python (python-oracledb, thin mode)

```python
import oracledb

dsn = (
    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=<host>)(PORT=2484))"
    "(CONNECT_DATA=(SID=ORCL))"
    "(SECURITY=(SSL_SERVER_DN_MATCH=TRUE)))"
)
# config_dir points at a directory holding the imported GovCloud RDS CA bundle
# (ca_cert_bundle_url) as the trust store the thin driver reads.
conn = oracledb.connect(user=username, password=password, dsn=dsn,
                        config_dir="/path/to/wallet")
```

The listener negotiates **TLS 1.2** (RDS Oracle has no 1.3) with the FIPS-validated
AEAD cipher `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`; `FIPS.SSLFIPS_140=TRUE` means
the client **must** offer a FIPS cipher or the handshake fails. See
[hardening-baseline.md](hardening-baseline.md).

> **Use a least-privilege user, not the master.** The bound credential is the
> instance master, which on Oracle is **DBA-class**. Connect as the master once to
> create a least-privilege application user, then run your app as that user.
> Creating in-database users is the customer's responsibility — see
> [limitations.md](limitations.md).
