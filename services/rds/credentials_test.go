package rds

import (
	"strconv"
	"testing"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/go-test/deep"
)

func TestFormatDBName(t *testing.T) {
	dbIdentifier := "db" + helpers.RandStrNoCaps(15)
	i := &RDSInstance{
		Database: dbIdentifier,
	}
	dbName1 := formatDBName(i.Database)
	if dbName1 != dbIdentifier {
		t.Fatalf("database name should be %s", dbIdentifier)
	}
	dbName2 := formatDBName(i.Database)
	if dbName1 != dbName2 {
		t.Fatalf("database names should be the same")
	}
}

func TestGetCredentials(t *testing.T) {
	testCases := map[string]struct {
		credentialUtils CredentialUtils
		rdsInstance     *RDSInstance
		password        string
		expectErr       bool
		expectedCreds   map[string]string
	}{
		"postgres": {
			credentialUtils: &RDSCredentialUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "postgres",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 5432,
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
			},
			password: "fake-pw",
			expectedCreds: map[string]string{
				"uri":      "postgres://user-1:fake-pw@host:5432/db1",
				"username": "user-1",
				"password": "fake-pw",
				"host":     "host",
				"port":     strconv.FormatInt(5432, 10),
				"db_name":  "db1",
				"name":     "db1",
			},
		},
		"unknown databse type": {
			credentialUtils: &RDSCredentialUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "foobar",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 5432,
				},
				credentialUtils: &RDSCredentialUtils{},
			},
			password:  "fake-pw",
			expectErr: true,
		},
		"database with replica": {
			credentialUtils: &RDSCredentialUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "postgres",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 5432,
				},
				ReplicaDatabaseHost: "replica-host",
				Database:            "db-1",
				credentialUtils:     &RDSCredentialUtils{},
			},
			password: "fake-pw",
			expectedCreds: map[string]string{
				"uri":          "postgres://user-1:fake-pw@host:5432/db1",
				"username":     "user-1",
				"password":     "fake-pw",
				"host":         "host",
				"port":         strconv.FormatInt(5432, 10),
				"db_name":      "db1",
				"name":         "db1",
				"replica_host": "replica-host",
				"replica_uri":  "postgres://user-1:fake-pw@replica-host:5432/db1",
			},
		},
		"oracle-ee": {
			credentialUtils: &RDSCredentialUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "oracle-ee",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 1521,
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
			},
			password: "fake-pw",
			expectedCreds: map[string]string{
				"uri":          "oracle://user-1:fake-pw@host:1521/ORCL",
				"jdbcUrl":      "jdbc:oracle:thin:@//host:1521/ORCL",
				"username":     "user-1",
				"password":     "fake-pw",
				"host":         "host",
				"port":         strconv.FormatInt(1521, 10),
				"service_name": "ORCL",
				"sid":          "ORCL",
				"db_name":      "ORCL",
				"name":         "ORCL",
				"ssl_required": "true",
			},
		},
		"oracle-se2": {
			credentialUtils: &RDSCredentialUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "oracle-se2",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 1521,
				},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
			},
			password: "fake-pw",
			expectedCreds: map[string]string{
				"uri":          "oracle://user-1:fake-pw@host:1521/ORCL",
				"jdbcUrl":      "jdbc:oracle:thin:@//host:1521/ORCL",
				"username":     "user-1",
				"password":     "fake-pw",
				"host":         "host",
				"port":         strconv.FormatInt(1521, 10),
				"service_name": "ORCL",
				"sid":          "ORCL",
				"db_name":      "ORCL",
				"name":         "ORCL",
				"ssl_required": "true",
			},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			creds, err := test.credentialUtils.getCredentials(test.rdsInstance, test.password)
			if err == nil && test.expectErr {
				t.Fatal("expected error, got nil")
			}
			if diff := deep.Equal(creds, test.expectedCreds); diff != nil {
				t.Error(diff)
			}
		})
	}
}

// TestPostgresMySQLCredentialsUnchangedByRefactor is a behavior-preservation
// guard (ADR-0003, PR review scope_steward finding): the RDSBaseline refactor must
// not change the postgres/mysql binding payloads. These are the exact shapes the
// broker produced before the refactor; a future change that alters them fails here.
func TestPostgresMySQLCredentialsUnchangedByRefactor(t *testing.T) {
	u := &RDSCredentialUtils{}
	cases := map[string]struct {
		dbType string
		port   int64
		want   map[string]string
	}{
		"postgres": {
			dbType: "postgres", port: 5432,
			want: map[string]string{
				"uri":      "postgres://user-1:fake-pw@host:5432/db1",
				"username": "user-1", "password": "fake-pw", "host": "host",
				"port": "5432", "db_name": "db1", "name": "db1",
			},
		},
		"mysql": {
			dbType: "mysql", port: 3306,
			want: map[string]string{
				"uri":      "mysql://user-1:fake-pw@host:3306/db1",
				"username": "user-1", "password": "fake-pw", "host": "host",
				"port": "3306", "db_name": "db1", "name": "db1",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			i := &RDSInstance{
				DbType:          tc.dbType,
				Username:        "user-1",
				Instance:        base.Instance{Host: "host", Port: tc.port},
				Database:        "db-1",
				credentialUtils: &RDSCredentialUtils{},
			}
			got, err := u.getCredentials(i, "fake-pw")
			if err != nil {
				t.Fatal(err)
			}
			if diff := deep.Equal(got, tc.want); diff != nil {
				t.Errorf("%s credentials changed by refactor: %v", name, diff)
			}
		})
	}
}

// TestOracleBindingDoesNotLeakAdminMarkers guards the STIG concern (#534): the
// Oracle binding payload must not advertise itself as an admin/master credential
// or leak any admin-only key, even though it currently reuses the master user.
func TestOracleBindingDoesNotLeakAdminMarkers(t *testing.T) {
	u := &RDSCredentialUtils{}
	i := &RDSInstance{
		DbType:          "oracle-ee",
		Username:        "user-1",
		Instance:        base.Instance{Host: "host", Port: 1521},
		Database:        "db-1",
		credentialUtils: &RDSCredentialUtils{},
	}
	creds, err := u.getCredentials(i, "fake-pw")
	if err != nil {
		t.Fatal(err)
	}
	forbiddenKeys := []string{
		"master_username", "master_password", "master_user_password",
		"admin", "admin_username", "admin_password", "is_master", "role",
	}
	for _, k := range forbiddenKeys {
		if _, present := creds[k]; present {
			t.Errorf("oracle binding must not expose admin key %q", k)
		}
	}
	// The Oracle binding must carry the expected machine-readable connection keys.
	for _, k := range []string{"uri", "jdbcUrl", "username", "password", "host", "port", "service_name", "sid", "ssl_required"} {
		if _, present := creds[k]; !present {
			t.Errorf("oracle binding missing expected key %q", k)
		}
	}
	// ssl_required must assert TLS.
	if creds["ssl_required"] != "true" {
		t.Errorf("oracle binding ssl_required = %q, want \"true\"", creds["ssl_required"])
	}
}
