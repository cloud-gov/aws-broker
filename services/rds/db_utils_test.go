package rds

import (
	"strconv"
	"testing"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/go-test/deep"
)

func TestGetCredentials(t *testing.T) {
	testCases := map[string]struct {
		dbUtils       DatabaseUtils
		rdsInstance   *RDSInstance
		password      string
		expectErr     bool
		expectedCreds map[string]string
	}{
		"postgres": {
			dbUtils: &RDSDatabaseUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "postgres",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 5432,
				},
				dbUtils: &MockDbUtils{
					mockFormattedDbName: "db1",
				},
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
			dbUtils: &RDSDatabaseUtils{},
			rdsInstance: &RDSInstance{
				DbType:   "foobar",
				Username: "user-1",
				Instance: base.Instance{
					Host: "host",
					Port: 5432,
				},
				dbUtils: &MockDbUtils{
					mockFormattedDbName: "db1",
				},
			},
			password:  "fake-pw",
			expectErr: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			creds, err := test.dbUtils.getCredentials(test.rdsInstance, test.password)
			if err == nil && test.expectErr {
				t.Fatal("expected error, got nil")
			}
			if diff := deep.Equal(creds, test.expectedCreds); diff != nil {
				t.Error(diff)
			}
		})
	}
}
