package rds

import "testing"

// engine_test.go — unit tests for the per-engine RDSBaseline abstraction
// (epic #519, WS3 #523). Pure, no AWS, no DB.

func TestBaselineFor(t *testing.T) {
	cases := []struct {
		dbType      string
		wantOK      bool
		wantEngine  string
		wantScheme  string
		wantVersion bool // SupportsEngineVersionUpdate
		wantHard    bool // BornHardened
	}{
		{"postgres", true, EnginePostgres, "postgres", true, false},
		{"mysql", true, EngineMySQL, "mysql", true, false},
		{"oracle-ee", true, EngineOracleEE, "oracle", false, true},
		{"oracle-se2", true, EngineOracleEE, "oracle", false, true},
		{"mariadb", false, "", "", false, false},
		{"", false, "", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.dbType, func(t *testing.T) {
			b, ok := baselineFor(tc.dbType)
			if ok != tc.wantOK {
				t.Fatalf("baselineFor(%q) ok = %v, want %v", tc.dbType, ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if got := b.Engine(); got != tc.wantEngine {
				t.Errorf("Engine() = %q, want %q", got, tc.wantEngine)
			}
			scheme, schemeOK := b.URIScheme()
			if !schemeOK || scheme != tc.wantScheme {
				t.Errorf("URIScheme() = (%q,%v), want (%q,true)", scheme, schemeOK, tc.wantScheme)
			}
			if got := b.SupportsEngineVersionUpdate(); got != tc.wantVersion {
				t.Errorf("SupportsEngineVersionUpdate() = %v, want %v", got, tc.wantVersion)
			}
			if got := b.BornHardened(); got != tc.wantHard {
				t.Errorf("BornHardened() = %v, want %v", got, tc.wantHard)
			}
		})
	}
}

func TestFormatDBNamePreservesLegacyBehaviorForPgMysql(t *testing.T) {
	// Behavior-preserving guard (ADR-0003): postgres/mysql DB-name formatting must
	// equal the historical strip-non-alnum behavior.
	cases := []struct{ in, want string }{
		{"db12345", "db12345"},
		{"DB-abc_09", "abc09"},
		{"a.b-c!d", "abcd"},
	}
	for _, tc := range cases {
		for _, eng := range []string{"postgres", "mysql"} {
			if got := formatDBNameForEngine(eng, tc.in); got != tc.want {
				t.Errorf("formatDBNameForEngine(%q,%q) = %q, want %q", eng, tc.in, got, tc.want)
			}
		}
		// legacy engine-agnostic helper must match too
		if got := formatDBName(tc.in); got != tc.want {
			t.Errorf("formatDBName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatDBNameOracleIsFixedUppercaseSID(t *testing.T) {
	for _, eng := range []string{"oracle-ee", "oracle-se2"} {
		for _, in := range []string{"db12345abcdef01", "anything", ""} {
			if got := formatDBNameForEngine(eng, in); got != oracleSID {
				t.Errorf("formatDBNameForEngine(%q,%q) = %q, want %q", eng, in, got, oracleSID)
			}
		}
	}
	// Oracle SID must satisfy RDS constraints: <=8 chars, uppercase alnum.
	if len(oracleSID) == 0 || len(oracleSID) > 8 {
		t.Errorf("oracleSID %q must be 1..8 chars", oracleSID)
	}
	for _, r := range oracleSID {
		if !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
			t.Errorf("oracleSID %q must be uppercase alnum", oracleSID)
		}
	}
}

func TestUnknownEngineFallsBackToLegacyFormatDBName(t *testing.T) {
	// An engine with no baseline must not panic and must strip like the legacy path.
	if got := formatDBNameForEngine("sqlserver-ex", "DB-x_9"); got != "x9" {
		t.Errorf("fallback formatDBNameForEngine = %q, want %q", got, "x9")
	}
}

func TestIsOracleEngine(t *testing.T) {
	for _, eng := range []string{"oracle-ee", "oracle-se2"} {
		if !isOracleEngine(eng) {
			t.Errorf("isOracleEngine(%q) = false, want true", eng)
		}
	}
	for _, eng := range []string{"postgres", "mysql", "oracle", ""} {
		if isOracleEngine(eng) {
			t.Errorf("isOracleEngine(%q) = true, want false", eng)
		}
	}
}
