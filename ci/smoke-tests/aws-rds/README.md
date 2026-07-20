## aws-rds

Quick and dirty Go program used in database broker smoke tests. Provisioning a
brokered database, binding this app to it, and having the app start successfully
proves the service is usable.

### Usage

1. `cf create-service <service> <plan-name> <instance-name>`
1. wait for service creation to finish
1. `cf push <app> --var rds-service=<instance-name>` with `DB_TYPE` set to the
   engine (`postgres`, `mysql`, or `oracle-se2`)
1. If the app starts successfully, your brokered database service was able to be
   written to.

### Drivers

- PostgreSQL: `github.com/lib/pq`
- MySQL: `github.com/go-sql-driver/mysql`
- Oracle: `github.com/sijms/go-ora/v2` — a **pure-Go** driver (registers as
  `oracle`). It needs **no** Oracle Instant Client / native libraries, so the app
  builds and runs on a plain `go_buildpack` with `CGO_ENABLED=0`.
