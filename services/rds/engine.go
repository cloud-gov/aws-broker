package rds

// engine.go — per-engine baseline abstraction (epic #519, WS3 #523).
//
// Historically the broker expressed engine behavior with the bare DbType string
// ("postgres", "mysql") threaded through ~15 scattered `if i.DbType == "..."`
// sites (see docs/oracle19c/current-state-analysis.md §10). Adding STIG-hardened
// Oracle 19c on top of that pattern would scatter security-relevant logic across
// the broker. This file introduces a single per-engine strategy — RDSBaseline —
// that those sites delegate to, and structured baseline data (see
// services/rds/baselines/) that a future CSB brokerpak can reuse (ADR-0003).
//
// The abstraction is introduced incrementally and behavior-preservingly: existing
// MySQL/Postgres behavior is expressed by baselines whose outputs equal today's
// hard-coded branches, so the existing regression tests stay green. Oracle adds a
// new baseline rather than new conditionals.

// Engine identifiers as they appear in catalog `dbType` and the AWS RDS `Engine`
// field. Oracle uses AWS's engine strings ("oracle-ee"/"oracle-se2").
const (
	EnginePostgres  = "postgres"
	EngineMySQL     = "mysql"
	EngineOracleEE  = "oracle-ee"
	EngineOracleSE2 = "oracle-se2"
)

// isOracleEngine reports whether an engine string is any supported Oracle edition.
func isOracleEngine(dbType string) bool {
	return dbType == EngineOracleEE || dbType == EngineOracleSE2
}

// RDSBaseline is the per-engine strategy the broker delegates engine-specific
// decisions to. Implementations must be pure (no I/O, no AWS calls) so they are
// trivially unit-testable and safe to call from any lifecycle phase; anything
// requiring AWS (e.g. resolving a parameter-group family) stays in the AWS client
// layer and is passed results, not the client.
//
// The interface is intentionally additive: it starts by covering the branch sites
// that Oracle needs (credentials/URI scheme, DB-name formatting, version-update
// capability, default parameters, log exports, create-param validation). Further
// sites (option groups, reconcile) are folded in as the Oracle baselines land.
type RDSBaseline interface {
	// Engine returns the canonical engine string this baseline handles.
	Engine() string

	// SupportsEngine reports whether this baseline handles the given dbType.
	// (Oracle's baseline handles both oracle-ee and oracle-se2.)
	SupportsEngine(dbType string) bool

	// URIScheme returns the connection-URI scheme used in binding credentials
	// (e.g. "postgres", "mysql", "oracle"), or ok=false if this engine does not
	// support broker-issued connection URIs.
	URIScheme() (scheme string, ok bool)

	// FormatDBName returns the logical database name / Oracle SID for the given
	// broker-generated database identifier, honoring engine constraints (Oracle
	// SID: <=8 chars, uppercase).
	FormatDBName(database string) string

	// SupportsEngineVersionUpdate reports whether `cf update-service -c '{"version":...}'`
	// may change the engine version for this engine.
	SupportsEngineVersionUpdate() bool

	// BornHardened reports whether an instance of this engine must be provisioned
	// with a broker-managed custom parameter group by default (Oracle: true —
	// it is born hardened; MySQL/Postgres: false — custom params are opt-in).
	BornHardened() bool

	// ValidateIdentifiers checks broker-generated identifiers (DBName/SID and
	// master username) against engine constraints, before any AWS call. Returns a
	// non-nil error describing the first violation. Engines with no special
	// constraints return nil.
	ValidateIdentifiers(dbName, username string) error

	// DefaultLogExports returns the CloudWatch Logs export types enabled by default
	// for this engine (e.g. Oracle: alert/audit/listener), or nil for engines with
	// no broker-imposed default.
	DefaultLogExports() []string

	// DefaultParameters returns the broker-managed custom parameter-group entries
	// this engine is born with (Oracle hardened baseline), keyed by parameter name.
	// Engines that are not born hardened return an empty map.
	DefaultParameters() (map[string]paramDetails, error)
}

// baselineRegistry maps every supported engine string to its baseline. Oracle's
// two editions share one baseline instance.
var baselineRegistry = func() map[string]RDSBaseline {
	m := map[string]RDSBaseline{}
	for _, b := range []RDSBaseline{
		postgresBaseline{},
		mysqlBaseline{},
		oracle19cBaseline{},
	} {
		// Register the canonical engine plus any alias the baseline supports.
		for _, eng := range []string{EnginePostgres, EngineMySQL, EngineOracleEE, EngineOracleSE2} {
			if b.SupportsEngine(eng) {
				m[eng] = b
			}
		}
	}
	return m
}()

// baselineFor returns the RDSBaseline for an engine string, or ok=false if the
// engine is not supported by the broker.
func baselineFor(dbType string) (RDSBaseline, bool) {
	b, ok := baselineRegistry[dbType]
	return b, ok
}
