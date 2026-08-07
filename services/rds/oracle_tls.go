package rds

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// oracle_tls.go — server-side TLS enablement for brokered Oracle. credentials.go
// publishes a TCPS descriptor on 2484, but RDS only opens that listener when an
// Oracle SSL option group is attached; this file provisions it (oracle-se2 only).
//
// Scope: encryption-in-transit only (SC-8 / SC-8(1) / SC-13).
//
// Known gap: the broker attaches the SSL option and opens 2484, but cannot deny
// plaintext 1521 — that security-group rule is owned by the platform
// (cg-provision/Terraform), outside the broker's IAM. Until it lands, 1521 stays
// open alongside 2484, which is an SC-8 assessor finding, so the plan is not
// customer-ready.

const (
	oracleSSLPort       int32 = 2484
	oracleSSLOptionName       = "SSL"

	// RDS default CA is RSA (rds-ca-rsa2048-g1); compatible with ECDHE_RSA
	// ciphers. An ECDSA-only suite would require the "ecc" CA family.
	oracleCACertFamily = "rsa"
)

// FedRAMP-Moderate posture: TLS 1.2 (RDS Oracle has no 1.3), a FIPS-validated
// RSA-CA-compatible AEAD suite, FIPS mode on.
var oracleSSLOptionSettings = []rdsTypes.OptionSetting{
	{Name: aws.String("SQLNET.SSL_VERSION"), Value: aws.String("1.2")},
	{Name: aws.String("SQLNET.CIPHER_SUITE"), Value: aws.String("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")},
	{Name: aws.String("FIPS.SSLFIPS_140"), Value: aws.String("TRUE")},
}

func isOracleEngine(dbType string) bool {
	return dbType == "oracle-se1" || dbType == "oracle-se2"
}

// oracleSSLCipherCACompatible fails closed on an ECDSA-only cipher over the RSA
// CA (which would otherwise fail opaquely at AWS); guards against a future edit
// weakening the shipped ECDHE_RSA suite.
func oracleSSLCipherCACompatible() error {
	var cipher string
	for _, s := range oracleSSLOptionSettings {
		if aws.ToString(s.Name) == "SQLNET.CIPHER_SUITE" {
			cipher = aws.ToString(s.Value)
			break
		}
	}
	if strings.Contains(cipher, "_ECDSA_") && strings.EqualFold(oracleCACertFamily, "rsa") {
		return fmt.Errorf("oracle SSL: cipher %q is ECDSA-only but the CA family is %q (RSA); use an ECDHE_RSA cipher or an ECC CA", cipher, oracleCACertFamily)
	}
	return nil
}

// oracleBaselineOptions returns the create-time option-group options for a
// brokered Oracle instance (the SSL/TCPS option). Returns nil for non-Oracle
// engines. Fails closed if the cipher/CA guard trips.
func oracleBaselineOptions(i *RDSInstance) ([]rdsTypes.OptionConfiguration, error) {
	if i == nil || !isOracleEngine(i.DbType) {
		return nil, nil
	}
	if err := oracleSSLCipherCACompatible(); err != nil {
		return nil, err
	}

	ssl := rdsTypes.OptionConfiguration{
		OptionName:     aws.String(oracleSSLOptionName),
		Port:           aws.Int32(oracleSSLPort),
		OptionSettings: oracleSSLOptionSettings,
	}
	if i.SecGroup != "" {
		ssl.VpcSecurityGroupMemberships = []string{i.SecGroup}
	}
	return []rdsTypes.OptionConfiguration{ssl}, nil
}
