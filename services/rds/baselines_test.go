package rds

import (
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// baselines_test.go — the embedded Oracle 19c baseline files must parse and carry
// the expected STIG-relevant content (epic #519, WS5/6/7). These run offline; they
// are development signal for the values, not compliance evidence (ADR-0005).

func TestOracleBaselineFilesParse(t *testing.T) {
	if _, err := loadOracleParameters(); err != nil {
		t.Fatalf("parameters.yml: %v", err)
	}
	if _, err := loadOracleLogExports(); err != nil {
		t.Fatalf("log_exports.yml: %v", err)
	}
	if _, err := loadOracleOptions(); err != nil {
		t.Fatalf("options.yml: %v", err)
	}
}

func TestOracleParameterBaselineContent(t *testing.T) {
	f, err := loadOracleParameters()
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]oracleParameter{}
	for _, p := range f.Parameters {
		byName[p.Name] = p
		// apply_method must be one of the two RDS values, and resolvable.
		if p.ApplyMethod != "immediate" && p.ApplyMethod != "pending-reboot" {
			t.Errorf("param %q has invalid apply_method %q", p.Name, p.ApplyMethod)
		}
		if _, err := getRdsApplyMethodEnum(p.ApplyMethod); err != nil {
			t.Errorf("param %q apply_method not resolvable: %v", p.Name, err)
		}
		if p.Value == "" {
			t.Errorf("param %q has empty value", p.Name)
		}
		if p.StigIntent == "" {
			t.Errorf("param %q missing stig_intent (reviewability)", p.Name)
		}
	}
	// Core STIG-relevant parameters must be present and hardened.
	wantHardened := map[string]string{
		"audit_trail":               "DB,EXTENDED",
		"audit_sys_operations":      "TRUE",
		"sec_case_sensitive_logon":  "TRUE",
		"remote_login_passwordfile": "NONE",
		"resource_limit":            "TRUE",
		"sql92_security":            "TRUE",
	}
	for name, want := range wantHardened {
		got, ok := byName[name]
		if !ok {
			t.Errorf("missing required hardened parameter %q", name)
			continue
		}
		if got.Value != want {
			t.Errorf("parameter %q = %q, want hardened %q", name, got.Value, want)
		}
	}
	// audit_trail is a static parameter — must be pending-reboot.
	if byName["audit_trail"].ApplyMethod != "pending-reboot" {
		t.Errorf("audit_trail must be pending-reboot (static), got %q", byName["audit_trail"].ApplyMethod)
	}
}

func TestOracleLogExportsBaseline(t *testing.T) {
	f, err := loadOracleLogExports()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"alert", "audit", "listener"} {
		if !slices.Contains(f.DefaultExports, want) {
			t.Errorf("default log exports missing %q", want)
		}
	}
	// Every default must be in the supported set.
	for _, d := range f.DefaultExports {
		if !slices.Contains(f.Supported, d) {
			t.Errorf("default export %q not in supported set %v", d, f.Supported)
		}
	}
	// High-volume/opt-in exports must NOT be on by default.
	for _, notDefault := range []string{"trace", "oemagent"} {
		if slices.Contains(f.DefaultExports, notDefault) {
			t.Errorf("%q should not be a default export", notDefault)
		}
	}
}

func TestOracleOptionsBaselineHasNoAttackSurfaceOptions(t *testing.T) {
	f, err := loadOracleOptions()
	if err != nil {
		t.Fatal(err)
	}
	// #535: the baseline must not enable attack-surface options.
	forbidden := []string{"XMLDB", "HTTP", "EXTPROC", "JVM", "JAVAVM", "APEX"}
	for _, opt := range f.Options {
		up := opt.Name
		for _, bad := range forbidden {
			if len(up) >= len(bad) && containsFold(up, bad) {
				t.Errorf("option %q looks like an attack-surface option (%q); must be justified", opt.Name, bad)
			}
		}
	}
}

func TestOracleSSLOptionBaseline(t *testing.T) {
	f, err := loadOracleOptions()
	if err != nil {
		t.Fatal(err)
	}
	if f.SSLPort != 2484 {
		t.Errorf("ssl_port = %d, want 2484", f.SSLPort)
	}
	if f.CACertFamily != "rsa" {
		t.Errorf("ca_cert_family = %q, want rsa", f.CACertFamily)
	}
	// The SSL option must carry TLS 1.2, a FedRAMP+FIPS+RSA-compatible cipher, and FIPS on.
	var ssl *oracleOption
	for i := range f.Options {
		if f.Options[i].Name == "SSL" {
			ssl = &f.Options[i]
		}
	}
	if ssl == nil {
		t.Fatal("SSL option missing from oracle options baseline")
	}
	if ssl.Port != 2484 {
		t.Errorf("SSL option port = %d, want 2484", ssl.Port)
	}
	want := map[string]string{
		"SQLNET.SSL_VERSION":  "1.2",
		"SQLNET.CIPHER_SUITE": "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
		"FIPS.SSLFIPS_140":    "TRUE",
	}
	got := map[string]string{}
	for _, s := range ssl.Settings {
		got[s.Name] = s.Value
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("SSL setting %s = %q, want %q", k, got[k], v)
		}
	}
	// The chosen cipher must be an ECDHE_RSA suite (RSA-CA compatible), not ECDSA-only.
	if strings.Contains(got["SQLNET.CIPHER_SUITE"], "_ECDSA_") {
		t.Errorf("SSL cipher %q is ECDSA-only; incompatible with the RSA CA", got["SQLNET.CIPHER_SUITE"])
	}
}

func TestOracleBaselineOptionsBuild(t *testing.T) {
	b, ok := baselineFor("oracle-se2")
	if !ok {
		t.Fatal("oracle-se2 baseline missing")
	}
	i := &RDSInstance{DbType: "oracle-se2"}
	i.SecGroup = "sg-abc123"
	opts, err := b.BaselineOptions(i)
	if err != nil {
		t.Fatalf("BaselineOptions error: %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("expected 1 baseline option (SSL), got %d", len(opts))
	}
	ssl := opts[0]
	if aws.ToString(ssl.OptionName) != "SSL" {
		t.Errorf("option name = %q, want SSL", aws.ToString(ssl.OptionName))
	}
	if aws.ToInt32(ssl.Port) != 2484 {
		t.Errorf("option port = %d, want 2484", aws.ToInt32(ssl.Port))
	}
	if len(ssl.VpcSecurityGroupMemberships) != 1 || ssl.VpcSecurityGroupMemberships[0] != "sg-abc123" {
		t.Errorf("SSL option must reference the instance SG, got %v", ssl.VpcSecurityGroupMemberships)
	}
	// pg/mysql have no baseline options.
	for _, eng := range []string{"postgres", "mysql"} {
		pgb, _ := baselineFor(eng)
		o, err := pgb.BaselineOptions(&RDSInstance{DbType: eng})
		if err != nil || len(o) != 0 {
			t.Errorf("%s BaselineOptions = (%v,%v), want (nil,nil)", eng, o, err)
		}
	}
}

func TestOracleBaselineWiredIntoEngine(t *testing.T) {
	b, ok := baselineFor("oracle-se2")
	if !ok {
		t.Fatal("oracle-se2 baseline missing")
	}
	// DefaultParameters must reflect the embedded file.
	params, err := b.DefaultParameters()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := params["audit_trail"]; !ok {
		t.Error("oracle DefaultParameters missing audit_trail")
	}
	if p := params["audit_trail"]; p.applyMethod != "pending-reboot" {
		t.Errorf("audit_trail applyMethod = %q, want pending-reboot", p.applyMethod)
	}
	// DefaultLogExports must reflect the embedded file.
	exports, err := b.DefaultLogExports()
	if err != nil {
		t.Fatalf("oracle DefaultLogExports error: %v", err)
	}
	if !slices.Contains(exports, "audit") {
		t.Errorf("oracle DefaultLogExports = %v, want to include audit", exports)
	}
	// Non-Oracle engines have no defaults (behavior preserved).
	pg, _ := baselineFor("postgres")
	pgExports, err := pg.DefaultLogExports()
	if err != nil {
		t.Fatalf("postgres DefaultLogExports error: %v", err)
	}
	if len(pgExports) != 0 {
		t.Error("postgres must have no default log exports")
	}
	pgParams, _ := pg.DefaultParameters()
	if len(pgParams) != 0 {
		t.Error("postgres must have no born-hardened default parameters")
	}
}

// containsFold reports whether s contains substr, case-insensitively.
func containsFold(s, substr string) bool {
	return len(substr) == 0 ||
		len(s) >= len(substr) &&
			indexFold(s, substr) >= 0
}

func indexFold(s, substr string) int {
	ls, lsub := len(s), len(substr)
	for i := 0; i+lsub <= ls; i++ {
		match := true
		for j := 0; j < lsub; j++ {
			cs, cb := s[i+j], substr[j]
			if 'a' <= cs && cs <= 'z' {
				cs -= 'a' - 'A'
			}
			if 'a' <= cb && cb <= 'z' {
				cb -= 'a' - 'A'
			}
			if cs != cb {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// TestOracleSSLBaselineIsFedRAMPCompliant is the CI gate for the SSL security
// invariants (S1: options.yml is our own committed file, so the values are enforced
// by this test rather than a runtime guard — matching how pg/mysql have no runtime
// param guard). It asserts the SHIPPED baseline is TLS 1.2 + FIPS on + a FedRAMP/FIPS
// allowlisted cipher; a weakened options.yml fails go test and never merges.
func TestOracleSSLBaselineIsFedRAMPCompliant(t *testing.T) {
	f, err := loadOracleOptions()
	if err != nil {
		t.Fatal(err)
	}
	s := f.sslSettings()

	if v := s["SQLNET.SSL_VERSION"]; v != "1.2" {
		t.Errorf("SQLNET.SSL_VERSION=%q, require exactly \"1.2\" (FedRAMP floor; RDS Oracle ceiling)", v)
	}
	if v := s["FIPS.SSLFIPS_140"]; !strings.EqualFold(v, "TRUE") {
		t.Errorf("FIPS.SSLFIPS_140=%q, require TRUE (SC-13)", v)
	}
	cipher, ok := s["SQLNET.CIPHER_SUITE"]
	if !ok || cipher == "" {
		t.Fatal("SQLNET.CIPHER_SUITE is required")
	}
	if _, ok := fedrampOracleSSLCiphers[cipher]; !ok {
		t.Errorf("cipher %q is not on the FedRAMP/FIPS allowlist", cipher)
	}
}

// TestOracleSSLCipherCACompatible covers the runtime compatibility guard kept on the
// provision path (option-b): an ECDSA-only cipher on the RSA CA is rejected before
// the AWS associate call.
func TestOracleSSLCipherCACompatible(t *testing.T) {
	ok := &oracleOptionsFile{
		CACertFamily: "rsa",
		Options: []oracleOption{{Name: "SSL", Settings: []oracleOptionSetting{
			{Name: "SQLNET.CIPHER_SUITE", Value: "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"},
		}}},
	}
	if err := oracleSSLCipherCACompatible(ok); err != nil {
		t.Errorf("RSA cipher on RSA CA should be compatible, got %v", err)
	}

	bad := &oracleOptionsFile{
		CACertFamily: "rsa",
		Options: []oracleOption{{Name: "SSL", Settings: []oracleOptionSetting{
			{Name: "SQLNET.CIPHER_SUITE", Value: "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"},
		}}},
	}
	if err := oracleSSLCipherCACompatible(bad); err == nil {
		t.Error("ECDSA cipher on RSA CA must be rejected")
	}
}
