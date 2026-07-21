package rds

import (
	"crypto/aes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
)

type CredentialUtils interface {
	generatePassword(salt string, password string, key string) (string, error)
	getPassword(salt string, password string, key string) (string, error)
	getCredentials(i *RDSInstance, password string) (map[string]string, error)
	generateCredentials(settings *config.Settings) (string, string, error)
}

// formatDBName returns the logical DB name / Oracle SID for a broker-generated
// database identifier. It delegates to the per-engine RDSBaseline (WS3 #523) so
// engine-specific constraints (e.g. Oracle SID <= 8 upper "ORCL") live in one
// place. Unknown engines fall back to the historical strip-non-alnum behavior so
// existing callers/tests are unaffected.
func formatDBNameForEngine(dbType, database string) string {
	if b, ok := baselineFor(dbType); ok {
		return b.FormatDBName(database)
	}
	return nonAlnumLower.ReplaceAllString(database, "")
}

// formatDBName preserves the original engine-agnostic signature used by callers
// that do not (yet) thread the engine through. It applies the historical
// postgres/mysql behavior. Oracle callers use formatDBNameForEngine.
func formatDBName(database string) string {
	return nonAlnumLower.ReplaceAllString(database, "")
}

type RDSCredentialUtils struct {
}

func (u *RDSCredentialUtils) generatePassword(salt string, password string, key string) (string, error) {
	if salt == "" {
		return "", errors.New("salt has to be set before writing the password")
	}

	iv, _ := base64.StdEncoding.DecodeString(salt)

	encrypted, err := helpers.Encrypt(password, key, iv)
	if err != nil {
		return "", err
	}

	return encrypted, nil
}

func (u *RDSCredentialUtils) getPassword(salt string, password string, key string) (string, error) {
	if salt == "" || password == "" {
		return "", errors.New("salt and password has to be set before getting the password")
	}

	iv, _ := base64.StdEncoding.DecodeString(salt)

	decrypted, err := helpers.Decrypt(password, key, iv)
	if err != nil {
		return "", err
	}

	return decrypted, nil
}

func (u *RDSCredentialUtils) getCredentials(i *RDSInstance, password string) (map[string]string, error) {
	baseline, ok := baselineFor(i.DbType)
	if !ok {
		return nil, errors.New("Cannot generate credentials for unsupported db type: " + i.DbType)
	}
	dbScheme, ok := baseline.URIScheme()
	if !ok {
		return nil, errors.New("Cannot generate credentials for unsupported db type: " + i.DbType)
	}

	// Oracle bindings connect by service name (SID), carry a JDBC thin URL, and
	// declare ssl_required; other engines use the historical scheme://.../dbname
	// URI. The logical name is engine-formatted (Oracle SID vs stripped name).
	dbName := baseline.FormatDBName(i.Database)

	if isOracleEngine(i.DbType) {
		return oracleCredentials(i, password, dbScheme, dbName)
	}

	uri := fmt.Sprintf(
		"%s://%s:%s@%s:%d/%s",
		dbScheme,
		i.Username,
		password,
		i.Host,
		i.Port,
		dbName,
	)

	credentials := map[string]string{
		"uri":      uri,
		"username": i.Username,
		"password": password,
		"host":     i.Host,
		"port":     strconv.FormatInt(i.Port, 10),
		"db_name":  dbName,
		"name":     dbName,
	}

	if i.ReplicaDatabaseHost != "" {
		credentials["replica_host"] = i.ReplicaDatabaseHost
		credentials["replica_uri"] = fmt.Sprintf(
			"%s://%s:%s@%s:%d/%s",
			dbScheme,
			i.Username,
			password,
			i.ReplicaDatabaseHost,
			i.Port,
			dbName,
		)
	}

	return credentials, nil
}

// oracleCredentials builds the Oracle-specific binding payload (WS9 #528, TLS #538).
// It returns machine-readable connection details for a bound app over TLS/TCPS:
// the SSL port (2484), a TCPS connect descriptor (uri + jdbcUrl with PROTOCOL=TCPS
// and SSL_SERVER_CERT_DN), ssl_server_dn_match, and the GovCloud RDS CA bundle URL
// the client must trust. Like the postgres/mysql plans, the broker returns the
// instance master credential per binding; the customer creates their own
// least-privilege in-database users (the intended shared-responsibility boundary).
// No admin/master marker key is exposed here.
//
// FAIL-CLOSED (security review): if the TLS SSL port cannot be resolved from the
// embedded baseline, this returns an error rather than falling back to the
// plaintext endpoint port — we never emit a binding that claims ssl_required=true
// while pointing the client at a plaintext listener.
//
// NOTE (#538): TLS-on-2484 requires the broker-provisioned SSL option group AND a
// platform security-group rule allowing 2484 / denying 1521 (a cg-provision
// dependency, #541). The plan is not customer-ready until that SG rule lands;
// ssl_required=true reflects the intended+configured posture.
func oracleCredentials(i *RDSInstance, password, scheme, serviceName string) (map[string]string, error) {
	// TLS listener port from the embedded SSL option baseline (2484). Fail closed
	// if unavailable — do NOT fall back to the plaintext endpoint port.
	sslPort, err := oracleSSLPort()
	if err != nil {
		return nil, fmt.Errorf("oracle TLS binding: cannot resolve SSL port, refusing to emit binding: %w", err)
	}
	if sslPort == 0 {
		return nil, errors.New("oracle TLS binding: SSL port is 0, refusing to emit a binding that would claim TLS on a plaintext port")
	}

	// Server cert DN for RDS Oracle TLS: the RDS-issued cert CN is the endpoint.
	// Enables ssl_server_dn_match to verify server identity (not just encrypt).
	certDN := fmt.Sprintf("C=US,ST=Washington,L=Seattle,O=Amazon.com,OU=RDS,CN=%s", i.Host)

	// TCPS connect descriptor (URI form) with DN match — EZConnect cannot express
	// PROTOCOL=TCPS or the cert DN, so we use the full DESCRIPTION form.
	descriptor := fmt.Sprintf(
		"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=%s)(PORT=%d))"+
			"(CONNECT_DATA=(SID=%s))"+
			"(SECURITY=(SSL_SERVER_CERT_DN=\"%s\")))",
		i.Host, sslPort, serviceName, certDN,
	)
	uri := fmt.Sprintf("%s://%s:%s@%s", scheme, i.Username, password, descriptor)
	jdbcURL := fmt.Sprintf("jdbc:oracle:thin:@%s", descriptor)

	return map[string]string{
		"uri":                 uri,
		"jdbcUrl":             jdbcURL,
		"username":            i.Username,
		"password":            password,
		"host":                i.Host,
		"port":                strconv.FormatInt(int64(sslPort), 10),
		"protocol":            "tcps",
		"service_name":        serviceName,
		"sid":                 serviceName,
		"db_name":             serviceName,
		"name":                serviceName,
		"ssl_required":        "true",
		"ssl_server_dn_match": "true",
		"ssl_server_cert_dn":  certDN,
		"ca_cert_bundle_url":  "https://truststore.pki.us-gov-west-1.rds.amazonaws.com/global/global-bundle.pem",
	}, nil
}

func (u *RDSCredentialUtils) generateCredentials(
	settings *config.Settings,
) (string, string, error) {
	salt := helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStrNoCaps(25)
	encrypted, err := u.generatePassword(salt, password, settings.EncryptionKey)
	if err != nil {
		return "", "", err
	}
	return salt, encrypted, err
}

func generateDatabaseName(
	settings *config.Settings,
) string {
	return settings.DbNamePrefix + helpers.RandStrNoCaps(15)
}

func buildUsername() string {
	return "u" + helpers.RandStrNoCaps(15)
}
