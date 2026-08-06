package rds

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/go-test/deep"
)

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

	expected := []rdsTypes.OptionConfiguration{
		{
			OptionName:                  aws.String(oracleSSLOptionName),
			Port:                        aws.Int32(oracleSSLPort),
			OptionSettings:              oracleSSLOptionSettings,
			VpcSecurityGroupMemberships: []string{"sg-1"},
		},
	}
	if diff := deep.Equal(opts, expected); diff != nil {
		t.Error(diff)
	}
}

// TestOracleSSLBaselineIsFedRAMPCompliant gates the SSL posture: TLS 1.2 + FIPS
// on + a FedRAMP/FIPS allowlisted cipher.
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

	// Exact-match settings: compare as a map so a failure prints the full diff.
	fixed := map[string]string{
		"SQLNET.SSL_VERSION": s["SQLNET.SSL_VERSION"],
		"FIPS.SSLFIPS_140":   s["FIPS.SSLFIPS_140"],
	}
	wantFixed := map[string]string{
		"SQLNET.SSL_VERSION": "1.2",  // FedRAMP floor; RDS Oracle ceiling
		"FIPS.SSLFIPS_140":   "TRUE", // SC-13
	}
	if diff := deep.Equal(fixed, wantFixed); diff != nil {
		t.Error(diff)
	}

	// Cipher is an allowlist membership check, not a single fixed value.
	cipher, ok := s["SQLNET.CIPHER_SUITE"]
	if !ok || cipher == "" {
		t.Fatal("SQLNET.CIPHER_SUITE missing from SSL baseline")
	}
	if _, ok := fedrampCiphers[cipher]; !ok {
		t.Errorf("cipher %q is not on the FedRAMP/FIPS allowlist", cipher)
	}
}

// TestOracleBaselineOptionsNoOpForNonOracle: postgres/mysql yield nil options.
func TestOracleBaselineOptionsNoOpForNonOracle(t *testing.T) {
	for _, eng := range []string{"postgres", "mysql", ""} {
		opts, err := oracleBaselineOptions(&RDSInstance{DbType: eng})
		if err != nil {
			t.Errorf("%q oracleBaselineOptions returned error %v, want nil", eng, err)
		}
		if diff := deep.Equal(opts, []rdsTypes.OptionConfiguration(nil)); diff != nil {
			t.Errorf("%q: %v", eng, diff)
		}
	}
}

// TestProvisionBaselineOptionGroup_Oracle exercises the create-time
// describe→create→modify→attach sequence for an Oracle instance.
func TestProvisionBaselineOptionGroup_Oracle(t *testing.T) {
	optionGroupNotFound := &rdsTypes.OptionGroupNotFoundFault{}
	mockRDS := &mockRDSClient{
		// not-found → the group is created
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

// TestProvisionBaselineOptionGroup_NoOpForNonOracle: postgres/mysql must not
// touch option groups at create time.
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
