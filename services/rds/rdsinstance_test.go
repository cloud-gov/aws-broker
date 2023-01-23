package rds

import (
	"testing"

	"github.com/18F/aws-broker/helpers"
)

func TestFormatDBName(t *testing.T) {
	i := RDSInstance{
		Database: "db" + helpers.RandStrNoCaps(15),
	}
	dbName1 := i.FormatDBName()
	dbName2 := i.FormatDBName()
	if dbName1 != dbName2 {
		t.Fatalf("database names should be the same")
	}
}
