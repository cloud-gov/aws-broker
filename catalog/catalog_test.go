package catalog

import (
	"testing"
)

var catalogData = []byte(`
rds:
  id: "db80ca29-2d1b-4fbc-aad3-d03c0bfa7593"
  name: "rds"
  description: "RDS Database Broker"
  bindable: true
  tags:
    - "database"
    - "RDS"
    - "postgresql"
    - "mysql"
  metadata:
    displayName: RDS Database Broker
    imageUrl:
    longDescription:
    providerDisplayName: RDS
    documentationUrl:
    supportUrl:
  plans:
    -
      id: "44d24fc7-f7a4-4ac1-b7a0-de82836e89a3"
      name: "shared-psql"
      description: "Shared infrastructure for Postgres DB"
      metadata:
        bullets:
          - "Shared RDS Instance"
          - "Postgres instance"
        costs:
          -
            amount:
              usd: 0
            unit: "MONTHLY"
        displayName: "Free Shared Plan"
      free: true
      adapter: shared
      dbType: sqlite3
      securityGroup: sg-123456
      subnetGroup: subnet-group
      tags:
        environment: "cf-env"
        client: "the client"
        service: "aws-broker"
`)

func TestInitCatalog(t *testing.T) {
	catalog := InitCatalog(catalogData, secretsData)
	if catalog == nil {
		t.Error("Did not read catalog")
	}
}
