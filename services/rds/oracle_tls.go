package rds

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// oracle_tls.go — the RDS-side change that actually makes brokered Oracle
// connections use TLS. The binding credentials (credentials.go) already publish a
// TCPS connect descriptor on port 2484, but that only tells a client HOW to
// connect over TLS; without the broker provisioning + attaching an Oracle SSL
// option group, RDS never opens the TCPS listener and the TLS endpoint does not
// exist. This file supplies that missing server-side piece for oracle-se2.
//
// Scope: TLS/encryption-in-transit only (SC-8 / SC-8(1) / SC-13). The broader
// Oracle hardening (parameter groups, log exports, identifier validation) lives
// elsewhere and is intentionally NOT included here.
//
// HONESTY / SCOPE NOTES:
//   - RDS Oracle serves TLS on a SEPARATE TCPS listener port (2484) via the SSL
//     option below — unlike postgres/mysql, which negotiate TLS on their standard
//     port. This port divergence is an AWS/Oracle architecture fact.
//   - The broker attaches this option group and expresses the SSL intent (port
//     2484, the plan security group), but it CANNOT make the connection TLS-only:
//     opening 2484 ingress and DENYING plaintext 1521 is an EC2 security-group
//     change owned by the platform (cg-provision/Terraform), outside the broker's
//     IAM. Leaving 1521 open alongside 2484 is an SC-8 assessor finding, so the
//     plan is NOT customer-ready until the platform SG rule lands.
//   - TLS version ceiling: RDS Oracle SSL supports TLS 1.2 only (no 1.3). 1.2 is
//     acceptable for FedRAMP Moderate.
//   - Cipher/CA compatibility: the default RDS CA is RSA (rds-ca-rsa2048-g1),
//     compatible with the ECDHE_RSA cipher below. ECDSA-only ciphers would require
//     the ECC CA and are rejected fail-closed before the AWS call.

const (
	// oracleSSLPort is the TCPS listener port RDS Oracle exposes for TLS.
	oracleSSLPort int32 = 2484

	// oracleSSLOptionName is the RDS Oracle option that turns on the TCPS listener.
	oracleSSLOptionName = "SSL"

	// oracleCACertFamily is the RDS CA family brokered Oracle instances use. An
	// "rsa" CA is compatible with TLS_ECDHE_RSA_* suites; an ECDSA-only suite would
	// need "ecc".
	oracleCACertFamily = "rsa"
)

// oracleSSLOptionSettings are the SQLNET/FIPS settings applied to the SSL option.
// These are the FedRAMP-Moderate posture: TLS 1.2, a FIPS-validated AEAD suite
// compatible with the RSA CA, and FIPS mode on (writes fips.ora SSLFIPS_140=TRUE).
var oracleSSLOptionSettings = []rdsTypes.OptionSetting{
	// TLS 1.2 only. RDS Oracle does not support 1.3 via the SSL option.
	{Name: aws.String("SQLNET.SSL_VERSION"), Value: aws.String("1.2")},
	// FedRAMP-compliant + FIPS + RSA-CA-compatible AEAD suite.
	{Name: aws.String("SQLNET.CIPHER_SUITE"), Value: aws.String("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")},
	// Force the RDS Oracle crypto module into FIPS 140 mode (SC-13).
	{Name: aws.String("FIPS.SSLFIPS_140"), Value: aws.String("TRUE")},
}

// isOracleEngine reports whether an engine string is a supported Oracle edition.
func isOracleEngine(dbType string) bool {
	return dbType == "oracle-se1" || dbType == "oracle-se2"
}

// oracleSSLCipherCACompatible is a cheap fail-closed guard: an ECDSA-only cipher on
// the RSA CA would fail opaquely at AWS when the option group is associated. The
// shipped cipher above is ECDHE_RSA (RSA-CA compatible); the guard protects against
// a future edit that weakens/misconfigures it.
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

// oracleBaselineOptions returns the option-group options a brokered Oracle instance
// is provisioned with at CREATE time: the SSL/TCPS option that enables TLS. The SSL
// listener is gated by the instance's security group. Returns nil for non-Oracle
// engines (postgres/mysql have no baseline option group). Fails closed if the
// cipher/CA guard trips.
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
	// The SSL/TCPS listener is gated by the instance's security group.
	if i.SecGroup != "" {
		ssl.VpcSecurityGroupMemberships = []string{i.SecGroup}
	}
	return []rdsTypes.OptionConfiguration{ssl}, nil
}
