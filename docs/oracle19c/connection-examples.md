# Oracle 19c — connecting a bound app

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.

After `cf bind-service my-app my-oracle`, the binding credentials appear in
`VCAP_SERVICES` under the `aws-rds` service. The Oracle binding payload
(`services/rds/credentials.go`, [#528](https://github.com/cloud-gov/aws-broker/issues/528)):

```json
{
  "uri": "oracle://APP_USER:REDACTED@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=my-oracle.abc.us-gov-west-1.rds.amazonaws.com)(PORT=2484))(CONNECT_DATA=(SID=ORCL))(SECURITY=(SSL_SERVER_CERT_DN=\"C=US,ST=Washington,L=Seattle,O=Amazon.com,OU=RDS,CN=my-oracle.abc.us-gov-west-1.rds.amazonaws.com\")))",
  "jdbcUrl": "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=my-oracle.abc.us-gov-west-1.rds.amazonaws.com)(PORT=2484))(CONNECT_DATA=(SID=ORCL))(SECURITY=(SSL_SERVER_CERT_DN=\"C=US,ST=Washington,L=Seattle,O=Amazon.com,OU=RDS,CN=my-oracle.abc.us-gov-west-1.rds.amazonaws.com\")))",
  "username": "APP_USER",
  "password": "REDACTED",
  "host": "my-oracle.abc.us-gov-west-1.rds.amazonaws.com",
  "port": "2484",
  "protocol": "tcps",
  "service_name": "ORCL",
  "sid": "ORCL",
  "db_name": "ORCL",
  "name": "ORCL",
  "ssl_required": "true",
  "ssl_server_dn_match": "true",
  "ssl_server_cert_dn": "C=US,ST=Washington,L=Seattle,O=Amazon.com,OU=RDS,CN=my-oracle.abc.us-gov-west-1.rds.amazonaws.com",
  "ca_cert_bundle_url": "https://truststore.pki.us-gov-west-1.rds.amazonaws.com/global/global-bundle.pem"
}
```

The binding advertises TLS/TCPS: `port` `2484`, `protocol` `tcps`, and a full
Oracle connect `DESCRIPTION` (not EZConnect) in both `uri` and `jdbcUrl` carrying
`PROTOCOL=TCPS` and `SSL_SERVER_CERT_DN`. `ssl_required=true` and
`ssl_server_dn_match=true` instruct the client to encrypt **and** verify the server
identity; `ca_cert_bundle_url` is the GovCloud RDS CA bundle the client must trust.
The instance uses the RDS default RSA CA (`rds-ca-rsa2048-g1`), compatible with the
ECDHE_RSA cipher. Verified offline + by go unit tests; not yet exercised against a
live GovCloud RDS TLS handshake (WS15).

> **TLS not yet reachable end-to-end (platform SG, [#541](https://github.com/cloud-gov/aws-broker/issues/541)).**
> The broker provisions the RDS Oracle SSL option group and the binding expresses
> the TCPS/2484 posture, but the broker **cannot** make the connection TLS-only:
> opening `2484` ingress and denying plaintext `1521` is a platform security-group
> change (cg-provision), tracked in [#541](https://github.com/cloud-gov/aws-broker/issues/541).
> Until that lands, `2484` is not yet reachable and plaintext `1521` remains open —
> so this plan is **not customer-ready**. `ssl_required=true` reflects the
> configured+intended posture, not a live-enforced one.

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
# open a tunnel to the TCPS listener (keep it running):
cf ssh -N -L 2484:<host>:2484 my-app
# then point your client at localhost:2484 using the TCPS DESCRIPTION from the
# binding (with HOST=<host> preserved so ssl_server_dn_match verifies the cert DN).
```

> Tunnelling to `2484` requires the platform security group to allow it
> ([#541](https://github.com/cloud-gov/aws-broker/issues/541)); until then only the
> plaintext `1521` listener is reachable.

`<host>`, `username`, `password` come from `cf env my-app`.

## JDBC (Java)

Wire the binding's `jdbcUrl` (a TCPS `DESCRIPTION`) into your app, import the
GovCloud RDS CA bundle into a truststore, and require server-DN matching:

```bash
# import the GovCloud RDS CA bundle (ca_cert_bundle_url from the binding):
wget https://truststore.pki.us-gov-west-1.rds.amazonaws.com/global/global-bundle.pem
keytool -importcert -alias rds-ca -file global-bundle.pem \
  -keystore truststore.jks -storepass "$TRUSTSTORE_PW" -noprompt
```

```java
String url = System.getenv("ORACLE_JDBC_URL"); // the TCPS jdbcUrl from the binding
// jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=..)(PORT=2484))
//   (CONNECT_DATA=(SID=ORCL))(SECURITY=(SSL_SERVER_CERT_DN="...")))
Properties p = new Properties();
p.put("user", username);
p.put("password", password);
p.put("oracle.net.ssl_server_dn_match", "true"); // ssl_server_dn_match=true
p.put("javax.net.ssl.trustStore", "truststore.jks");
p.put("javax.net.ssl.trustStorePassword", System.getenv("TRUSTSTORE_PW"));
Connection c = DriverManager.getConnection(url, p);
```

> The listener negotiates **TLS 1.2** (RDS Oracle has no 1.3) with a FIPS-validated
> AEAD cipher (`TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`); `FIPS.SSLFIPS_140=TRUE`
> means the client **must** offer a FIPS/FedRAMP cipher or the handshake fails.

## SQLcl / SQL*Plus

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

## TLS

Encryption-in-transit is provided by an **RDS Oracle SSL option group** the broker
provisions and attaches at create, which serves TLS on a dedicated **TCPS listener
(port 2484)**: TLS 1.2, cipher `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`, and
`FIPS.SSLFIPS_140=TRUE` (SC-8 / SC-8(1) / SC-13; DISA Oracle 19c STIG V-270579,
V-270571). The binding advertises this posture (`port=2484`, `protocol=tcps`,
`ssl_required=true`, `ssl_server_dn_match=true`, `ssl_server_cert_dn`,
`ca_cert_bundle_url`). RDS Oracle SSL supports **TLS 1.2 only** (no 1.3) — acceptable
for FedRAMP Moderate. Configure your driver to trust the GovCloud RDS CA bundle and
require server-DN matching (examples above).

> **Not TLS-only until the platform SG lands ([#541](https://github.com/cloud-gov/aws-broker/issues/541)).**
> The broker attaches the SSL option and expresses the 2484 intent, but cannot
> open `2484` ingress or deny plaintext `1521` — that is a platform security-group
> change (cg-provision). Until [#541](https://github.com/cloud-gov/aws-broker/issues/541)
> lands, `1521` plaintext remains reachable and TLS on `2484` is not yet
> connectable, so the plan is **not customer-ready**. Verified offline + go unit
> tests; the live TLS handshake is not yet exercised against GovCloud RDS (WS15).

> **Use a least-privilege user, not the master.** The bound credential is the
> instance master, which on Oracle is **DBA-class**. Connect as the master once to
> create a least-privilege application user, then run your app as that user — see
> [binding.md](binding.md). Creating in-database users is the customer's
> responsibility.
