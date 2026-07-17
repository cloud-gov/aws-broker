package rds

import (
	"fmt"
	"regexp"
	"strings"
)

// engine_baselines.go — concrete RDSBaseline implementations (epic #519, WS3 #523).
//
// postgres/mysql baselines encode TODAY's behavior exactly (behavior-preserving,
// per ADR-0003); oracle19cBaseline adds Oracle 19c. Keeping them in one file makes
// the per-engine differences easy to review side by side.

// nonAlnumLower strips everything that is not [a-z0-9]; this is the historical
// formatDBName behavior for postgres/mysql (see credentials.go).
var nonAlnumLower = regexp.MustCompile(`(i?)[^a-z0-9]`)

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

// ---------------------------------------------------------------------------
// Oracle 19c (oracle-ee / oracle-se2)
// ---------------------------------------------------------------------------

// oracleSID is the fixed logical DB name / SID used for brokered Oracle instances.
// RDS Oracle DBName (SID) must be <= 8 chars, uppercase, and cannot be a reserved
// word; a fixed "ORCL" mirrors the historical broker behavior and the smoke-test
// client, and keeps the DBInstanceIdentifier (the random db<...> value) as the
// unique per-instance identifier.
const oracleSID = "ORCL"

type oracle19cBaseline struct{}

func (oracle19cBaseline) Engine() string { return EngineOracleEE }
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

// oracleReservedUsernames are identifiers RDS rejects (or reserves) as the Oracle
// master username. RDS explicitly disallows these; provisioning with one fails at
// the AWS call, so we reject fail-closed before that (#535).
var oracleReservedUsernames = map[string]struct{}{
	"SYS": {}, "SYSTEM": {}, "ADMIN": {}, "RDSADMIN": {}, "RDS_ADMIN": {},
	"PUBLIC": {}, "OUTLN": {}, "DBSNMP": {}, "AUDSYS": {}, "GSMADMIN_INTERNAL": {},
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
		if !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
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
