package rds

import "regexp"

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
