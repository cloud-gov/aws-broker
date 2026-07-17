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
		return oracleCredentials(i, password, dbScheme, dbName), nil
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

// oracleCredentials builds the Oracle-specific binding payload (WS9 #528).
// It returns machine-readable connection details for a bound app: a URI using
// the "oracle" scheme with the service name as the path, a JDBC thin URL, the
// service name/SID, and ssl_required. NOTE (#534): the broker currently returns
// the instance master credential for every binding; a per-binding least-privilege
// Oracle app user is a tracked follow-up. No admin/master flag is exposed here.
func oracleCredentials(i *RDSInstance, password, scheme, serviceName string) map[string]string {
	// EZConnect-style URI: oracle://user:pass@host:port/SERVICE_NAME
	uri := fmt.Sprintf(
		"%s://%s:%s@%s:%d/%s",
		scheme,
		i.Username,
		password,
		i.Host,
		i.Port,
		serviceName,
	)
	// JDBC thin service-name form: jdbc:oracle:thin:@//host:port/SERVICE_NAME
	jdbcURL := fmt.Sprintf(
		"jdbc:oracle:thin:@//%s:%d/%s",
		i.Host,
		i.Port,
		serviceName,
	)

	return map[string]string{
		"uri":          uri,
		"jdbcUrl":      jdbcURL,
		"username":     i.Username,
		"password":     password,
		"host":         i.Host,
		"port":         strconv.FormatInt(i.Port, 10),
		"service_name": serviceName,
		"sid":          serviceName,
		"db_name":      serviceName,
		"name":         serviceName,
		"ssl_required": "true",
	}
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
