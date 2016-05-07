package catalog

import (
	"testing"
)

var secretsData = []byte(`
rds:
  service_id: "db80ca29-2d1b-4fbc-aad3-d03c0bfa7593"
  plans:
  -
    plan_id: "44d24fc7-f7a4-4ac1-b7a0-de82836e89a3"
    url: "test"
    username: "theuser"
    password: "thepassword"
    db_name: "db_name"
    db_type: "sqlite3"
    ssl_mode: "disable"
    port: 55
`)

func TestInitSecrets(t *testing.T) {
	secrets := InitSecrets(secretsData)
	if secrets == nil {
		t.Error("Did not read catalog")
	}
}
