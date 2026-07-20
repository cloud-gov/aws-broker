package rds

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// engine_baselines.go — concrete RDSBaseline implementations (epic #519, WS3 #523).
//
// postgres/mysql baselines encode TODAY's behavior exactly (behavior-preserving,
// per ADR-0003); oracle19cBaseline adds Oracle 19c. Keeping them in one file makes
// the per-engine differences easy to review side by side.

// nonAlnumLower strips everything that is not [a-z0-9]; this is the historical
// formatDBName behavior for postgres/mysql (see credentials.go). The prior code
// carried a `(i?)` prefix — a no-op capturing group (not the `(?i)` inline flag),
// dropped here since the class already excludes uppercase; behavior is unchanged.
var nonAlnumLower = regexp.MustCompile(`[^a-z0-9]`)

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

type postgresBaseline struct{}

func (postgresBaseline) Engine() string                    { return EnginePostgres }
func (postgresBaseline) SupportsEngine(dbType string) bool { return dbType == EnginePostgres }
func (postgresBaseline) URIScheme() (string, bool)         { return "postgres", true }
func (postgresBaseline) FormatDBName(database string) string {
	return nonAlnumLower.ReplaceAllString(database, "")
}
func (postgresBaseline) SupportsEngineVersionUpdate() bool { return true }
func (postgresBaseline) BornHardened() bool                { return false }
func (postgresBaseline) ValidateIdentifiers(string, string) error {
	// No broker-imposed engine constraints beyond the shared generator; RDS
	// enforces the rest.
	return nil
}
func (postgresBaseline) DefaultLogExports() ([]string, error) { return nil, nil }
func (postgresBaseline) DefaultParameters() (map[string]paramDetails, error) {
	return map[string]paramDetails{}, nil
}
func (postgresBaseline) BaselineOptions(*RDSInstance) ([]rdsTypes.OptionConfiguration, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

type mysqlBaseline struct{}

func (mysqlBaseline) Engine() string                    { return EngineMySQL }
func (mysqlBaseline) SupportsEngine(dbType string) bool { return dbType == EngineMySQL }
func (mysqlBaseline) URIScheme() (string, bool)         { return "mysql", true }
func (mysqlBaseline) FormatDBName(database string) string {
	return nonAlnumLower.ReplaceAllString(database, "")
}
func (mysqlBaseline) SupportsEngineVersionUpdate() bool { return true }
func (mysqlBaseline) BornHardened() bool                { return false }
func (mysqlBaseline) ValidateIdentifiers(string, string) error {
	return nil
}
func (mysqlBaseline) DefaultLogExports() ([]string, error) { return nil, nil }
func (mysqlBaseline) DefaultParameters() (map[string]paramDetails, error) {
	return map[string]paramDetails{}, nil
}
func (mysqlBaseline) BaselineOptions(*RDSInstance) ([]rdsTypes.OptionConfiguration, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Oracle 19c (oracle-se2 — Standard Edition 2, License Included)
// ---------------------------------------------------------------------------

// oracleSID is the fixed logical DB name / SID used for brokered Oracle instances.
// RDS Oracle DBName (SID) must be <= 8 chars, uppercase, and cannot be a reserved
// word; a fixed "ORCL" mirrors the historical broker behavior and the smoke-test
// client, and keeps the DBInstanceIdentifier (the random db<...> value) as the
// unique per-instance identifier.
const oracleSID = "ORCL"

type oracle19cBaseline struct{}

// Engine reports the canonical Oracle engine string: oracle-se2. The shipped
// offering is SE2 + License Included (ADR-0004); Enterprise Edition is not
// supported.
func (oracle19cBaseline) Engine() string { return EngineOracleSE2 }
func (oracle19cBaseline) SupportsEngine(dbType string) bool {
	return isOracleEngine(dbType)
}

// URIScheme: Oracle connection URIs use the "oracle" scheme; the path component is
// the service name (SID), not a logical database name (handled by the binding
// formatter in credentials.go).
func (oracle19cBaseline) URIScheme() (string, bool) { return "oracle", true }

// FormatDBName returns the Oracle SID. RDS constrains the Oracle DBName to <= 8
// uppercase chars; we use a stable "ORCL".
func (oracle19cBaseline) FormatDBName(string) string { return oracleSID }

// SupportsEngineVersionUpdate: Oracle in-place version updates are not enabled in
// this first iteration (they require careful option-group/parameter-group major-
// version handling); tracked for a follow-up. Provisioning pins the plan version.
func (oracle19cBaseline) SupportsEngineVersionUpdate() bool { return false }

// BornHardened: Oracle is provisioned with a broker-managed hardened parameter
// group by default (ADR-0003; unlike the MySQL/Postgres opt-in pattern).
func (oracle19cBaseline) BornHardened() bool { return true }

// oracleReservedUsernames are identifiers RDS rejects/reserves as the Oracle
// master username, plus Oracle-supplied built-in schema accounts a tenant must not
// be able to shadow (privilege/schema-search-path risk). RDS blocks some of these
// server-side, but we reject fail-closed before the AWS call (#524, #535, PR review
// findings). Case-insensitive. Not exhaustive of all 100+ Oracle internal schemas,
// but covers the privileged/security-relevant and commonly-present ones.
var oracleReservedUsernames = map[string]struct{}{
	// RDS / master-adjacent
	"SYS": {}, "SYSTEM": {}, "ADMIN": {}, "RDSADMIN": {}, "RDS_ADMIN": {},
	"PUBLIC": {}, "OUTLN": {}, "DBSNMP": {}, "AUDSYS": {}, "GSMADMIN_INTERNAL": {},
	// Security / privileged option schemas
	"DVSYS": {}, "DVF": {}, "LBACSYS": {}, "OLS$INTERNAL": {}, "SYSKM": {},
	"SYSBACKUP": {}, "SYSDG": {}, "SYSRAC": {}, "SYS$UMF": {}, "REMOTE_SCHEDULER_AGENT": {},
	// Built-in feature schemas (shadowing risk)
	"CTXSYS": {}, "MDSYS": {}, "XDB": {}, "ORDSYS": {}, "ORDDATA": {}, "ORDPLUGINS": {},
	"SI_INFORMTN_SCHEMA": {}, "WMSYS": {}, "OJVMSYS": {}, "OLAPSYS": {}, "DMSYS": {},
	"EXFSYS": {}, "APEX_PUBLIC_USER": {}, "APEX_INSTANCE_ADMIN_USER": {},
	"FLOWS_FILES": {}, "MDDATA": {}, "SPATIAL_CSW_ADMIN_USR": {}, "SPATIAL_WFS_ADMIN_USR": {},
	"ANONYMOUS": {}, "DIP": {}, "ORACLE_OCM": {}, "XS$NULL": {}, "GGSYS": {},
	"DGPDB_INT": {}, "GSMCATUSER": {}, "GSMUSER": {}, "GSMROOTUSER": {},
	"APPQOSSYS": {}, "AUDSYS$": {}, "PDBADMIN": {},
}

// oracleUsernameRe: RDS Oracle master username must start with a letter and
// contain only letters, digits, and underscores.
var oracleUsernameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// ValidateIdentifiers enforces RDS Oracle constraints fail-closed before the AWS
// call: DBName/SID <=8 uppercase alnum; master username 8..30 chars, valid charset,
// not a reserved word (#524, #535).
func (oracle19cBaseline) ValidateIdentifiers(dbName, username string) error {
	if len(dbName) == 0 || len(dbName) > 8 {
		return fmt.Errorf("oracle SID/DBName %q must be 1..8 characters", dbName)
	}
	for _, r := range dbName {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return fmt.Errorf("oracle SID/DBName %q must be uppercase alphanumeric", dbName)
		}
	}
	if l := len(username); l < 8 || l > 30 {
		return fmt.Errorf("oracle master username must be 8..30 characters, got %d", l)
	}
	if !oracleUsernameRe.MatchString(username) {
		return fmt.Errorf("oracle master username %q must start with a letter and contain only letters, digits, underscores", username)
	}
	if _, reserved := oracleReservedUsernames[strings.ToUpper(username)]; reserved {
		return fmt.Errorf("oracle master username %q is reserved and cannot be used", username)
	}
	return nil
}

// DefaultLogExports returns the Oracle default CloudWatch log-export set from the
// embedded baseline (baselines/oracle19c/log_exports.yml, WS7 #527). Unlike a
// silent nil, a parse failure is returned as an error so the provision path fails
// closed rather than provisioning with no audit-log posture (#519 review C5).
func (oracle19cBaseline) DefaultLogExports() ([]string, error) {
	f, err := loadOracleLogExports()
	if err != nil {
		return nil, err
	}
	return f.DefaultExports, nil
}

// DefaultParameters returns the Oracle hardened parameter-group baseline from the
// embedded baseline (baselines/oracle19c/parameters.yml, WS5 #525), keyed by name.
func (oracle19cBaseline) DefaultParameters() (map[string]paramDetails, error) {
	f, err := loadOracleParameters()
	if err != nil {
		return nil, err
	}
	out := make(map[string]paramDetails, len(f.Parameters))
	for _, p := range f.Parameters {
		out[p.Name] = paramDetails{
			value:       p.Value,
			applyMethod: p.ApplyMethod,
		}
	}
	return out, nil
}

// BaselineOptions builds the Oracle SSL option (TLS/TCPS) from the embedded
// baseline (baselines/oracle19c/options.yml, #519/#538), referencing the
// instance's security group for the TCPS listener. Includes a fail-closed
// cipher/CA compatibility guard: an ECDSA-only cipher requires the ECC CA, so
// pairing it with the default RSA CA is rejected before the AWS call (which would
// otherwise fail opaquely at option-group associate time). Returns nil for an
// empty baseline.
func (oracle19cBaseline) BaselineOptions(i *RDSInstance) ([]rdsTypes.OptionConfiguration, error) {
	f, err := loadOracleOptions()
	if err != nil {
		return nil, err
	}
	if len(f.Options) == 0 {
		return nil, nil
	}

	// Fail-closed cipher/CA guard (#538 review): ECDSA-only ciphers need the ECC
	// CA; the RDS default is RSA. Reject the mismatch here rather than letting the
	// AWS associate call fail.
	if cipher, ok := f.cipherSuiteFor("SSL"); ok {
		if strings.Contains(cipher, "_ECDSA_") && strings.EqualFold(f.CACertFamily, "rsa") {
			return nil, fmt.Errorf(
				"oracle SSL cipher %q is ECDSA-only but the instance CA family is %q (RSA); "+
					"use an ECDHE_RSA cipher or an ECC CA", cipher, f.CACertFamily)
		}
	}

	out := make([]rdsTypes.OptionConfiguration, 0, len(f.Options))
	for _, opt := range f.Options {
		cfg := rdsTypes.OptionConfiguration{
			OptionName: aws.String(opt.Name),
		}
		if opt.Port != 0 {
			cfg.Port = aws.Int32(opt.Port)
		}
		// The SSL/TCPS listener is gated by the instance's security group.
		if i != nil && i.SecGroup != "" {
			cfg.VpcSecurityGroupMemberships = []string{i.SecGroup}
		}
		for _, s := range opt.Settings {
			cfg.OptionSettings = append(cfg.OptionSettings, rdsTypes.OptionSetting{
				Name:  aws.String(s.Name),
				Value: aws.String(s.Value),
			})
		}
		out = append(out, cfg)
	}
	return out, nil
}

// oracleSSLPort returns the TCPS port from the embedded options baseline (2484),
// or 0 if unavailable. Used by the binding to publish the TLS port instead of the
// plaintext endpoint port.
func oracleSSLPort() int32 {
	f, err := loadOracleOptions()
	if err != nil {
		return 0
	}
	return f.SSLPort
}
