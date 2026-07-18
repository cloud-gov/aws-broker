// #cgo LDFLAGS: -L${SRCDIR}/include
package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/cloudfoundry-community/go-cfenv"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "gopkg.in/goracle.v2"
)

func main() {
	appEnv, err := cfenv.Current()
	if err != nil {
		panic("not in cloud foundry")
	}

	svcs := appEnv.Services
	dbType := os.Getenv("DB_TYPE")
	if dbType == "" {
		panic("DB_TYPE needs to be set")
	}

	svcName := os.Getenv("SERVICE_NAME")
	if svcName == "" {
		panic("SERVICE_NAME needs to be set")
	}

	svc, err := svcs.WithName(svcName)
	if err != nil {
		panic(err)
	}

	switch {
	// NOTE: use strings.Contains(dbType, keyword) — the args were previously
	// reversed (strings.Contains("oracle", dbType)), which asks whether the
	// literal "oracle" contains e.g. "oracle-ee" → false, so the DB test silently
	// never ran and the smoke test passed without touching the database. Oracle
	// plans pass DB_TYPE=oracle-ee (epic #519), which exposed the bug.
	case strings.Contains(dbType, "postgres"):
		openAndTest("postgres", svc.Credentials["uri"].(string))
	case strings.Contains(dbType, "mysql"):
		openAndTest("mysql", fmtMysql(svc))
	case strings.Contains(dbType, "oracle"):
		// The goracle driver registers itself under the name "goracle" and expects
		// an EZConnect DSN (user/pass@host:port/service), NOT the oracle:// URI the
		// broker emits. Build it from the discrete binding fields (epic #519).
		openAndTest("goracle", fmtOracle(svc))
	default:
		panic("unsupported DB_TYPE: " + dbType)
	}

	fmt.Printf("tested %s database, things look good, starting web server now.", dbType)

	http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), nil)
}

// set up the mysql connection strings.
func fmtMysql(svc *cfenv.Service) string {
	cfg := mysql.NewConfig()
	var ok bool

	if cfg.User, ok = svc.CredentialString("username"); !ok {
		panic("cannot parse username in mysql config")
	}
	if cfg.Passwd, ok = svc.CredentialString("password"); !ok {
		panic("cannot parse password in mysql config")
	}
	if cfg.DBName, ok = svc.CredentialString("db_name"); !ok {
		panic("cannot parse db_name in mysql config")
	}

	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s", svc.Credentials["host"].(string))

	return cfg.FormatDSN()
}

// fmtOracle builds an EZConnect DSN for the goracle driver from the broker's
// Oracle binding fields (username/password/host/port/service_name), rather than
// the oracle:// URI (which goracle does not parse). Epic #519.
func fmtOracle(svc *cfenv.Service) string {
	user, ok := svc.CredentialString("username")
	if !ok {
		panic("cannot parse username in oracle config")
	}
	pass, ok := svc.CredentialString("password")
	if !ok {
		panic("cannot parse password in oracle config")
	}
	host, ok := svc.CredentialString("host")
	if !ok {
		panic("cannot parse host in oracle config")
	}
	port, ok := svc.CredentialString("port")
	if !ok {
		panic("cannot parse port in oracle config")
	}
	service, ok := svc.CredentialString("service_name")
	if !ok {
		panic("cannot parse service_name in oracle config")
	}
	// goracle EZConnect: user/password@host:port/service_name
	return fmt.Sprintf("%s/%s@%s:%s/%s", user, pass, host, port, service)
}

func openAndTest(dbType string, dsn interface{}) {
	db, err := sql.Open(dbType, fmt.Sprintf("%v", dsn))
	if err != nil {
		panic(err)
	}

	if dbType == "goracle" {
		oracleSql(db)
		return
	}

	// create the table.
	if _, err := db.Exec("create table smoke (id integer, name text)"); err != nil {
		panic(err)
	}

	// insert into the table.
	if _, err := db.Exec("insert into smoke values (?, 'smoke')", 1); err != nil {
		panic(err)
	}

	f := os.Getenv("ENABLE_FUNCTIONS")
	yes, err := strconv.ParseBool(f)
	if yes {
		// test a function.
		if _, err := db.Exec("create function hello(id INT) returns CHAR(50) return 'foobar'", 1); err != nil {
			panic(err)
		}
	}

	// cleanup
	if _, err := db.Exec("drop table smoke", 1); err != nil {
		panic(err)
	}
}

// because it uses different types and stuff.
func oracleSql(db *sql.DB) {
	if _, err := db.Exec("CREATE TABLE smoke (id integer, name varchar2(10))"); err != nil {
		panic(err)
	}

	if _, err := db.Exec("INSERT INTO smoke VALUES (1, 'smoke')"); err != nil {
		panic(err)
	}

	if _, err := db.Exec("DROP TABLE smoke"); err != nil {
		panic(err)
	}
}
