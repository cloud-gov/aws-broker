# aws-broker RDS lifecycle — current-state analysis (Oracle 19c prep)

> Workstream 0 of the Oracle 19c epic ([#519](https://github.com/cloud-gov/aws-broker/issues/519), [#520](https://github.com/cloud-gov/aws-broker/issues/520)).
> Snapshot of how RDS brokering works **today** so Oracle insertion points and
> risks are explicit before any code changes. Citations are `file:line` against
> the branch point of `feat/oracle-19c-stig-brokered-rds` (main @ `3e41f27`).

## TL;DR

There is **no per-engine abstraction**. The engine is a bare `DbType` string
(`"postgres"`, `"mysql"`) threaded from the catalog plan → `RDSInstance` → async
worker → the AWS SDK input, with **~15 scattered `if i.DbType == "..."` / `switch`
sites**. Oracle existed historically and was reverted; the reverted commits are a
usable template. Adding Oracle 19c means touching every branching site in
[§10](#10-engine-specific-branching-inventory). The cleanest path is to introduce
an `RDSBaseline` engine strategy ([#523](https://github.com/cloud-gov/aws-broker/issues/523))
behind the existing service-level adapters and move the branching into it.

## 1. Catalog & plan representation

Plans are parsed from `catalog.yml` under the top-level `rds:` key
(`catalog/catalog.go`), into `RDSPlan` (`catalog/rds.go`):

```go
type RDSPlan struct {
	domain.ServicePlan    `yaml:",inline" validate:"required"`
	Adapter               string            `yaml:"adapter" validate:"required"`
	InstanceClass         string            `yaml:"instanceClass"`
	DbType                string            `yaml:"dbType" validate:"required"`
	DbVersion             string            `yaml:"dbVersion"`
	LicenseModel          string            `yaml:"licenseModel"`   // ← already wired; unused by pg/mysql
	Tags                  map[string]string `yaml:"tags" validate:"required"`
	Redundant             bool              `yaml:"redundant"`
	Encrypted             bool              `yaml:"encrypted"`
	StorageType           string            `yaml:"storage_type"`
	AllocatedStorage      int64             `yaml:"allocatedStorage"`
	BackupRetentionPeriod int64             `yaml:"backup_retention_period" validate:"required"`
	SubnetGroup           string            `yaml:"subnetGroup" validate:"required"`
	SecurityGroup         string            `yaml:"securityGroup" validate:"required"`
	ApprovedMajorVersions []string          `yaml:"approvedMajorVersions"`
	ReadReplica           bool              `yaml:"read_replica"`
}
```

- `RDSService.FetchPlan(planID)` looks a plan up; `CheckVersion` validates a
  requested version against `ApprovedMajorVersions` (empty list ⇒ allow, let AWS
  reject).
- A representative Postgres plan (`catalog-test.yml`, `micro-psql`) and MySQL plan
  (`small-mysql`) carry `dbType`, `dbVersion`, `instanceClass`, `allocatedStorage`,
  `encrypted`, `storage_type: gp3`, `backup_retention_period`, `subnetGroup`,
  `securityGroup`, `approvedMajorVersions`, and `tags`. **No plan carries
  `licenseModel` today** — that field is Oracle's entry point.

### Oracle history (reverted, reusable as template)
- `049847b` removed a `medium-oracle-se2` plan (`dbType: oracle-se2`,
  `licenseModel: license-included`, `db.t3.medium`, dedicated
  `oracle_security_group`) plus SQL Server plans.
- `2e3ce4b` made `formatDBName(database, dbType)` return `"ORCL"` for
  `oracle-se1`/`oracle-se2`; `bd70535` added `dbScheme = "oracle"` to
  `getCredentials`. **All reverted** (`566985c`, `aea8dd7`, `29128b6`).
- A `goracle` smoke-test client still lingers at `ci/smoke-tests/aws-rds/main.go`.

> For 19c the plan uses `dbType: oracle-se2` (Standard Edition 2, decided in
> ADR-0004), `dbVersion: "19...."`, and **`licenseModel: license-included`**
> (License Included, per ADR-0004), with a dedicated Oracle security group and private subnet
> group. The catalog plan is named `oracle-se2-license-included-dev`.

## 2. Create lifecycle (async, River-backed)

`rdsBroker.CreateInstance` (`services/rds/broker.go`) → unmarshal + `Options.Validate`
→ `FetchPlan` → `plan.CheckVersion` → `NewRDSInstance().init(...)` (`rdsinstance.go`)
→ `dedicatedDBAdapter.createDB` (`rds.go`), which **enqueues a River job** rather
than calling AWS synchronously:

```go
func (d *dedicatedDBAdapter) createDB(i *RDSInstance, plan *catalog.RDSPlan) (base.InstanceState, error) {
	// writes async CreateOp/InstanceInProgress message, opens a tx,
	// riverClient.InsertTx(ctx, sqlTx, &CreateArgs{Instance: i, Plan: plan}, nil), commit
	return base.InstanceInProgress, nil
}
```

The broker persists the instance `State = InstanceInProgress` and returns; CF polls
`LastOperation` (`broker.go`) which reads async job state
(`AsyncOperationRequired(CreateOp) == true`).

**Worker** (`create_worker.go`): `CreateKind = "rds-create"`, `CreateArgs{Instance, Plan}`,
`Work → asyncCreateDB`: decrypt password → **`prepareCreateDbInput`** →
`rds.CreateDBInstance` → `waitForDbReady` waiter → optional read replica → write
`InstanceReady`. Workers registered in `main.go`.

**`prepareCreateDbInput` is the single most important Oracle insertion point.**
Engine-relevant fields already present:

```go
params := &rds.CreateDBInstanceInput{
	DBName:             aws.String(formatDBName(i.Database)), // ← Oracle SID ≤8/upper "ORCL"
	Engine:             aws.String(i.DbType),                 // ← "oracle-ee" / "oracle-se2"
	MasterUsername:     &i.Username,                          // ← Oracle username rules differ
	MasterUserPassword: &password,
	// ...
}
if i.DbVersion != "" { params.EngineVersion = aws.String(i.DbVersion) }
if i.LicenseModel != "" { params.LicenseModel = aws.String(i.LicenseModel) } // ← License Included for Oracle SE2
if len(i.EnabledCloudwatchLogGroupExports) > 0 { params.EnableCloudwatchLogsExports = ... }
// parameterGroupClient.ProvisionNewCustomParameterGroup(i, rdsTags)
```

## 3. DB naming & identifiers

`credentials.go`:
- `formatDBName(database string)` (line 22) — **engine-agnostic today**; strips
  non-`[a-z0-9]`. The Oracle `"ORCL"` branch was reverted.
- `generateDatabaseName` = `settings.DbNamePrefix + RandStrNoCaps(15)` →
  used as both `DBInstanceIdentifier` and (via `formatDBName`) the logical `DBName`.
- `buildUsername` = `"u" + RandStrNoCaps(15)`.
- `generateCredentials` = `RandStrNoCaps(25)`, AES-encrypted with a per-instance salt.

**No per-engine name validation exists.** Oracle needs: `DBName`/SID ≤ 8 chars,
uppercase (reverted code used fixed `"ORCL"`); master-username reserved-word rules.
`DBInstanceIdentifier` (`db<15rand>`) is fine as-is. Restoring the engine-aware
`formatDBName(database, dbType)` signature touches ~6 callers (`create_worker.go`,
`credentials.go`, `parameter_group.go` ×2, `option_group.go` ×2) — [#524](https://github.com/cloud-gov/aws-broker/issues/524).

## 4. Parameter groups (`parameter_group.go`)

- `awsParameterGroupClient`, custom-group prefix `"cg-aws-broker-"`.
- **`needCustomParameters(i)`** — entirely engine-branched `if`s (MySQL functions /
  binlog / general+slowquery exports; Postgres pg_cron / pg logging). Returns false
  for any other engine ⇒ **no custom group** would be created for Oracle without a
  new branch.
- **`getParameterGroupFamily`** — engine-**generic**: calls
  `DescribeDBEngineVersions(Engine, EngineVersion, IncludeAll=true)` and reads
  `DBParameterGroupFamily`. **Works for Oracle unchanged** (returns e.g.
  `oracle-se2-19`).
- **`getNewParameters`** — MySQL/Postgres-specific blocks; the params map is
  **keyed by engine string** (`customparams[i.DbType]`), so Oracle needs its own
  entry or no params are applied.
- Existing user params are preserved (`getAllCustomParameters`, merge, new wins).
- Apply methods per-param (`types.go`): `immediate` vs `pending-reboot`.
- **`ReconcileRDSInstanceParameterGroup`** matches MySQL/Postgres keys only — Oracle
  params would be dropped on modify unless added here.
- Cleanup/delete engine-agnostic.

> Design principle for Oracle ([#525](https://github.com/cloud-gov/aws-broker/issues/525)):
> Oracle is **born with a hardened baseline parameter group by default** — unlike
> the MySQL opt-in-only pattern.

## 5. Option groups (`option_group.go`)

- `awsOptionsGroupClient`, prefix `"cg-aws-broker-"`.
- `ProvisionOrModifyCustomOptionGroup` **only acts on instances that already have a
  custom option group attached** — it exists mainly to rebuild the group on a
  major-version upgrade. The broker **never proactively creates** an option group on
  provision. In practice only MySQL/MariaDB use it (audit plugin).
- `CreateOptionGroup` input uses `EngineName: i.DbType` + `MajorEngineVersion`
  (generic). Reconcile/delete/cleanup present.

> Oracle is option-group-heavy (audit, native network encryption, timezone, etc.),
> so this passive model likely needs extension to **create** an Oracle baseline
> option group at provision — [#526](https://github.com/cloud-gov/aws-broker/issues/526).

## 6. Credentials & binding

**Model: master creds only — one master user per instance, reused for every binding.
No per-binding user.** (`MasterUsername`/`MasterUserPassword` set at create.)

Credentials are a `map[string]string` built in `RDSCredentialUtils.getCredentials`
(`credentials.go`), which **rejects Oracle today**:

```go
switch i.DbType {
case "postgres", "mysql":
	dbScheme = i.DbType
default:
	return nil, errors.New("Cannot generate credentials for unsupported db type: " + i.DbType) // line 68
}
uri := fmt.Sprintf("%s://%s:%s@%s:%d/%s", dbScheme, i.Username, password, i.Host, i.Port, dbName)
```

- Password AES-encrypted (`helpers.Encrypt`, `settings.EncryptionKey`) + per-instance
  salt, stored in the broker's Postgres DB (gorm).
- `BindInstance` is **synchronous**; returns the map as `VCAP_SERVICES.<svc>.credentials`
  (`uri` → buildpack `DATABASE_URL`).

> Oracle needs `dbScheme = "oracle"` and a service-name path (not a logical db name),
> plus `jdbcUrl`, `sid`/`service_name`, `ssl_required`
> ([#528](https://github.com/cloud-gov/aws-broker/issues/528)). The
> master-cred-per-binding reuse is a STIG concern tracked in
> [#534](https://github.com/cloud-gov/aws-broker/issues/534); first PR ships parity.

## 7. Validation (`validate.go` + `Options`)

`Options.Validate` runs on create and modify. Field validators: binary-log-format
(MySQL concept, **not engine-gated**), storage type (`"" | gp3`), long-query-time,
pg-query-logging ranges, backup-retention bounds, `AllocatedStorage <= MaxAllocatedStorage`.
The only engine gate on update is `broker.go` forbidding an engine switch. Version
approval via `plan.CheckVersion`.

- **No engine dispatch in validation.** Oracle should add allowlist-style create/update
  validation ([#535](https://github.com/cloud-gov/aws-broker/issues/535)).
- `hasEngineVersionUpdate` (`rdsinstance.go`) currently returns true only for
  postgres/mysql — Oracle version updates silently disallowed until extended.

## 8. CloudWatch log exports

- `EnabledCloudwatchLogGroupExports` on `RDSInstance`, set from
  `Options.EnableCloudWatchLogGroupExports`.
- Create: `params.EnableCloudwatchLogsExports`. Modify:
  `CloudwatchLogsExportConfiguration.EnableLogTypes`.
- MySQL couples `general`/`slowquery` exports to parameter-group params; Postgres/Oracle
  do not. Log-type strings are passed through, **not validated per engine**.

> Oracle log types (`alert`, `audit`, `listener`, `trace`) work at the API level;
> mostly catalog/docs + a default set — [#527](https://github.com/cloud-gov/aws-broker/issues/527).

## 9. Tests & mocks

- **No live AWS.** RDS client is the interface `RDSClientInterface` (`types.go`);
  tests inject `mockRDSClient` (`mocks_test.go`) with canned results + call counters.
- Sub-clients mocked: `mockParameterGroupClient`, `mockOptionGroupClient`,
  `mockCredentialUtils`. Adapter faked via `mockDBAdapter` when `Environment == "test"`.
- DB layer: in-memory sqlite (`testutil.TestDbInit` + gorm AutoMigrate); River via
  `testutil.GetRiverClient`.
- **Pattern for Oracle tests:** build `RDSInstance{DbType:"oracle-se2", DbVersion:"19...",
  Database:"db1"}`, feed `mockRDSClient{dbEngineVersions:[...MajorEngineVersion/
  DBParameterGroupFamily...]}`, assert on the captured `*rds.CreateDBInstanceInput`
  (mirror the `prepareCreateDbInput` and `option_group` tests).
- CI (`ci/run_tests.sh`) is `go test ./...` at root then `cd cmd/tasks && go test ./...`.
  **No vet/lint gate.** Smoke tests are the only live-AWS path (separate).

## 10. Engine-specific branching inventory

Every site an `RDSBaseline` per-engine strategy must cover:

| # | Location | Branch | Purpose |
|---|----------|--------|---------|
| 1 | `credentials.go` `getCredentials` | `switch DbType {postgres,mysql; default: error}` | URI scheme; **rejects Oracle** |
| 2 | `credentials.go` `formatDBName` | (engine-agnostic now) | DBName/SID format — Oracle ≤8 upper `ORCL` |
| 3 | `credentials.go` `generateDatabaseName`/`buildUsername` | identifier + master username gen | Oracle rules differ |
| 4 | `parameter_group.go` `needCustomParameters` | 4 engine-gated `if`s | whether a custom param group is needed |
| 5 | `parameter_group.go` `getNewParameters` | mysql/postgres blocks; map keyed by engine | which params + apply method |
| 6 | `parameter_group.go` `createOrModifyCustomParameterGroup` | `range customparams[DbType]` | engine-keyed param lookup |
| 7 | `parameter_group.go` `ReconcileRDSInstanceParameterGroup` | mysql/pg key matching | read live params back |
| 8 | `create_worker.go` `prepareCreateDbInput` | `formatDBName`, `Engine`, `EngineVersion`, `LicenseModel` | CreateDBInstance input |
| 9 | `modify_worker.go` | `EngineVersion`, `AllowMajorVersionUpgrade` | ModifyDBInstance input |
| 10 | `rdsinstance.go` `hasEngineVersionUpdate` | postgres\|mysql only | gates version-update (Oracle excluded) |
| 11 | `rdsinstance.go` `setPgQueryLogging` | Postgres version parsing | PG-only |
| 12 | `option_group.go` | `EngineName=DbType`, `getMajorEngineVersion` | option-group create/family |
| 13 | `broker.go` | forbid `newPlan.DbType != existing.DbType` | engine switch guard (keep) |
| 14 | `broker.go` | `plan.CheckVersion` | approved-version gate (generic) |
| 15 | `catalog/rds.go` + `catalog-*.yml` | `dbType` / `licenseModel` per plan | catalog data — add Oracle plan |

The only existing abstractions are **service-level** (`dbAdapter`,
`parameterGroupClient`, `optionGroupClient`, `CredentialUtils`), not engine-level.
The `RDSBaseline` strategy slots in behind them, dispatched by `i.DbType`, covering
rows 1–12.

## Recommended Oracle insertion points

1. **Catalog** — add `oracle-se2-license-included-dev` plan (SE2 + License Included, private, encrypted). ([#522](https://github.com/cloud-gov/aws-broker/issues/522))
2. **`RDSBaseline`** — new engine strategy; refactor pg/mysql behind it unchanged, add `oracle19cBaseline`. ([#523](https://github.com/cloud-gov/aws-broker/issues/523))
3. **`credentials.go`** — Oracle URI scheme + engine-aware `formatDBName`. ([#524](https://github.com/cloud-gov/aws-broker/issues/524), [#528](https://github.com/cloud-gov/aws-broker/issues/528))
4. **`hasEngineVersionUpdate`** — include Oracle (or per-engine capability flag).
5. **`needCustomParameters`/`getNewParameters`/reconcile** — Oracle hardened baseline. ([#525](https://github.com/cloud-gov/aws-broker/issues/525))
6. **Option group** — create Oracle baseline at provision if needed. ([#526](https://github.com/cloud-gov/aws-broker/issues/526))
7. **Tests** — Oracle table cases against `mockRDSClient` with an `oracle-se2-19` family. ([#531](https://github.com/cloud-gov/aws-broker/issues/531))

## Verify commands

```bash
go build ./...                                  # clean today
go test ./services/rds/... -run <Name> -count=1 # targeted, sqlite + mocks, no AWS
ci/run_tests.sh                                 # go test ./... x2 (no vet/lint gate)
```
