package rds

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// oracle_tls_test.go — the SSL/TCPS option baseline is the RDS-side change that
// makes brokered Oracle connections actually use TLS. These tests assert the
// FedRAMP-Moderate posture is shipped and that the create-time provisioning path
// creates + attaches the option group.

func sslSettingsMap(opt rdsTypes.OptionConfiguration) map[string]string {
	m := map[string]string{}
	for _, s := range opt.OptionSettings {
		m[aws.ToString(s.Name)] = aws.ToString(s.Value)
	}
	return m
}

func TestOracleBaselineOptionsBuildsSSL(t *testing.T) {
	i := &RDSInstance{DbType: "oracle-se2", SecGroup: "sg-1"}
	opts, err := oracleBaselineOptions(i)
	if err != nil {
		t.Fatalf("oracleBaselineOptions error: %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("expected 1 baseline option (SSL), got %d", len(opts))
	}
	ssl := opts[0]
	if aws.ToString(ssl.OptionName) != "SSL" {
		t.Errorf("option name = %q, want SSL", aws.ToString(ssl.OptionName))
	}
	if aws.ToInt32(ssl.Port) != 2484 {
		t.Errorf("SSL option port = %d, want 2484", aws.ToInt32(ssl.Port))
	}
	if len(ssl.VpcSecurityGroupMemberships) != 1 || ssl.VpcSecurityGroupMemberships[0] != "sg-1" {
		t.Errorf("SSL option must reference the instance SG, got %v", ssl.VpcSecurityGroupMemberships)
	}
}

// TestOracleSSLBaselineIsFedRAMPCompliant is the CI gate for the SSL security
// posture: TLS 1.2 + FIPS on + a FedRAMP/FIPS allowlisted cipher. A weakened
// oracle_tls.go fails go test and never merges.
func TestOracleSSLBaselineIsFedRAMPCompliant(t *testing.T) {
	fedrampCiphers := map[string]struct{}{
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384": {},
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256": {},
	}

	opts, err := oracleBaselineOptions(&RDSInstance{DbType: "oracle-se2"})
	if err != nil {
		t.Fatalf("oracleBaselineOptions error: %v", err)
	}
	s := sslSettingsMap(opts[0])

	if v := s["SQLNET.SSL_VERSION"]; v != "1.2" {
		t.Errorf("SQLNET.SSL_VERSION=%q, require exactly \"1.2\" (FedRAMP floor; RDS Oracle ceiling)", v)
	}
	if v := s["FIPS.SSLFIPS_140"]; v != "TRUE" {
		t.Errorf("FIPS.SSLFIPS_140=%q, require TRUE (SC-13)", v)
	}
	cipher, ok := s["SQLNET.CIPHER_SUITE"]
	if !ok || cipher == "" {
		t.Fatal("SQLNET.CIPHER_SUITE missing from SSL baseline")
	}
	if _, ok := fedrampCiphers[cipher]; !ok {
		t.Errorf("cipher %q is not on the FedRAMP/FIPS allowlist", cipher)
	}
}

// TestOracleBaselineOptionsNoOpForNonOracle: postgres/mysql have no baseline
// option group, so their create path is unchanged (nil options).
func TestOracleBaselineOptionsNoOpForNonOracle(t *testing.T) {
	for _, eng := range []string{"postgres", "mysql", ""} {
		opts, err := oracleBaselineOptions(&RDSInstance{DbType: eng})
		if err != nil || opts != nil {
			t.Errorf("%q oracleBaselineOptions = (%v,%v), want (nil,nil)", eng, opts, err)
		}
	}
}

// TestProvisionBaselineOptionGroup_Oracle exercises the create-time
// describe→create→modify→attach sequence for an Oracle instance.
func TestProvisionBaselineOptionGroup_Oracle(t *testing.T) {
	optionGroupNotFound := &rdsTypes.OptionGroupNotFoundFault{}
	mockRDS := &mockRDSClient{
		// First describe (does the target group exist?) returns not-found so the
		// group is created.
		describeOptionGroupsErrs: []error{optionGroupNotFound},
		dbEngineVersions: []rdsTypes.DBEngineVersion{
			{MajorEngineVersion: aws.String("19")},
		},
	}
	o := newTestOptionGroupClient(mockRDS)

	i := &RDSInstance{
		Database:  "db1",
		DbType:    "oracle-se2",
		DbVersion: "19.0.0.0.ru-2024-01.rur-2024-01.r1",
		SecGroup:  "sg-1",
	}

	if err := o.ProvisionBaselineOptionGroup(i, nil); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	expectedName := "cg-aws-broker-ORCL-option-19"
	if i.OptionGroupName != expectedName {
		t.Errorf("expected OptionGroupName %q, got %q", expectedName, i.OptionGroupName)
	}
	if mockRDS.createOptionGroupInput == nil {
		t.Fatal("expected CreateOptionGroup to be called")
	}
	if got := aws.ToString(mockRDS.createOptionGroupInput.EngineName); got != "oracle-se2" {
		t.Errorf("expected EngineName oracle-se2, got %q", got)
	}
	if mockRDS.modifyOptionGroupInput == nil {
		t.Fatal("expected ModifyOptionGroup to be called to add the SSL option")
	}
	var sslFound bool
	for _, opt := range mockRDS.modifyOptionGroupInput.OptionsToInclude {
		if aws.ToString(opt.OptionName) == "SSL" {
			sslFound = true
		}
	}
	if !sslFound {
		t.Errorf("expected SSL option to be added, got %v", mockRDS.modifyOptionGroupInput.OptionsToInclude)
	}
}

// TestProvisionBaselineOptionGroup_NoOpForNonOracle: postgres/mysql must not touch
// option groups at create time.
func TestProvisionBaselineOptionGroup_NoOpForNonOracle(t *testing.T) {
	mockRDS := &mockRDSClient{}
	o := newTestOptionGroupClient(mockRDS)

	i := &RDSInstance{Database: "db1", DbType: "postgres", DbVersion: "16.13"}
	if err := o.ProvisionBaselineOptionGroup(i, nil); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if i.OptionGroupName != "" {
		t.Errorf("expected no option group for postgres, got %q", i.OptionGroupName)
	}
	if mockRDS.createOptionGroupInput != nil {
		t.Error("expected CreateOptionGroup not to be called for postgres")
	}
}
