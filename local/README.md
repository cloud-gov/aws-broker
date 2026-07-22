# Local Oracle 19c test harness

> **⚠️ DEVELOPMENT SIGNAL ONLY — NOT COMPLIANCE EVIDENCE.**
> Everything here is for fast iteration. Authoritative STIG evidence comes only
> from running `cg-oracle-database-19c-stig-overlay` against a real brokered
> GovCloud RDS instance (the gated dev proof).

This lets you test the Oracle work **without any AWS access**, in three layers you
can use independently. Written for **macOS on Apple Silicon (arm64)**.

## TL;DR

> Run these from the **repo root** (`aws-broker/`); `-C local` points make at
> `local/Makefile`. If you're already in `local/`, drop `-C local` (e.g. `make doctor`).

```bash
make -C local doctor        # 0. check prerequisites
make -C local unit          # 1. Go unit tests (fast, no Docker)
make -C local moto-up moto-smoke moto-down   # 2. broker flow vs mock AWS RDS
make -C local oracle-up assess harden assess # 3. real Oracle + SQL hardening
make -C local down          # tear everything down
```

`make -C local quickstart` prints this menu any time.

## 0. Prerequisites (macOS arm64 / Linux)

Run `make -C local doctor` — it tells you exactly what's present/missing and the
command to install each. Summary:

| Requirement | Needed for | How |
|------|-----------|---------|
| **Docker** (running) | layers 2 & 3 | Docker Desktop, or `brew install colima docker && colima start` |
| **go** | layer 1 (unit tests) | `brew install go` |
| **C compiler** (`cc`/`gcc`/`clang`) | layer 1 — cgo builds `go-sqlite3` | `xcode-select --install` (macOS) or `apt-get install -y gcc` (Linux) |
| `aws` CLI | layer 2 `moto-smoke` only | `brew install awscli` (macOS) or `apt-get install -y awscli` (Linux) |
| **`cg-oracle-database-19c-stig-overlay` cloned as a sibling** | layer 3 `assess`/`harden` | `git clone` it next to `aws-broker` (so `../../cg-oracle-database-19c-stig-overlay/hardening/sql` resolves), or pass `SQL_DIR=…` |
| `cinc-auditor` (via Docker) | running the STIG overlay locally (optional) | run it from the `cincproject/auditor` container — do **not** install cinc-workstation (needs root); see §3 |
| `sqlplus` | **not required** | `assess`/`harden` run `sqlplus` *inside* the container via `docker exec` |

You do **not** need Oracle Instant Client or `sqlplus` on your Mac — layer 3 runs
`sqlplus` inside the `cg-oracle-free` container. Your clone layout for layer 3:

```
<workspace>/
├── aws-broker/                            (this repo)
└── cg-oracle-database-19c-stig-overlay/   (sibling — provides hardening/sql/)
```

## 1. Unit tests (fast, no Docker) — the everyday loop

```bash
make -C local unit          # go test ./... + cmd/tasks, with test config wired
make -C local unit-oracle   # just the Oracle-tagged tests, verbose
```

The broker's test suite needs `secrets.yml`/`catalog.yml` present; `make unit`
copies them from the committed `*-test.yml` files first (they're gitignored, so
this is safe and idempotent). This is the layer to run while editing Go code.

## 2. Broker flow vs a mock AWS RDS control plane (moto)

[moto](https://docs.getmoto.org/) mocks the AWS RDS **control plane** so the broker's
create / parameter-group / option-group calls can be exercised with **no real AWS**.

```bash
make -C local moto-up       # start moto on http://localhost:5000
make -C local moto-smoke    # create an Oracle instance + param + option group via the AWS API
make -C local moto-down
```

`moto-smoke` asserts the AWS API **accepts** the exact shape the broker builds
(`oracle-se2` 19c, encrypted, License Included, private; the `oracle-se2-19`
parameter group; the option group). To point the broker/tests themselves at moto,
override the AWS SDK endpoint (`--endpoint-url http://localhost:5000` for the CLI,
or `BaseEndpoint` in the Go client).

> moto does **not** run an Oracle engine, apply parameters, or do a TLS handshake —
> it proves request shape, not RDS behavior.

## 3. Real local Oracle for SQL hardening/assessment

[`gvenzl/oracle-free`](https://github.com/gvenzl/oci-oracle-free) runs a **real
Oracle engine natively on arm64** — used to develop and idempotency-test the SQL
hardening/assessment scripts and to run the overlay's `oracledb_session` controls.
Requires the overlay repo cloned as a sibling (see Prerequisites); `sqlplus` runs
inside the container, so nothing extra on your Mac.

```bash
make -C local oracle-up     # start oracle-free, wait until healthy (first pull ~mins)
make -C local assess        # run assessment SQL  -> local/reports/ (labeled dev signal)
make -C local harden        # apply allowed hardening (idempotent)
make -C local assess        # re-assess: the DEFAULT profile limits + unified audit
                            #   policies now report [PASS] in 90_validate
```

The container auto-seeds (from `init/`) a **non-`SYS`** privileged app user that
mirrors the RDS master-user privilege model, plus a deliberately-weak state. Note
`harden` only touches the DEFAULT profile + audit policies (10/20/30); the
PUBLIC-grant + network checks (40/50) are **detect-only** and never auto-remediate,
and the seeded `weak_profile`/`seed_weak` artifacts are on a non-DEFAULT profile
that hardening intentionally leaves alone. Reports land in `local/reports/`
(gitignored), each labeled *development signal only*.

**Optional fidelity pass** — a self-built Oracle **19c EE** image (closer to the
brokered engine than 23c Free). You must build/tag `oracle/database:19.3.0-ee`
yourself from [oracle/docker-images](https://github.com/oracle/docker-images)
first, then `make -C local oracle19c-up`. It publishes on **1522** with service
`ORCLPDB1` and does **not** create `APPUSER`, so target it explicitly, e.g.
`make -C local assess ORACLE_CONN="SYS/<pw>@//localhost:1522/ORCLPDB1 as sysdba"`
(this fidelity path is for maintainers; the default `oracle-free` flow above is the
supported one).

**Optional — run the STIG overlay** against the local DB: point its
`oracledb_session` inputs at `localhost:1521/FREEPDB1` with the seeded app user.
Run cinc-auditor **via Docker** (image `cincproject/auditor`) rather than
installing cinc-workstation locally (the workstation install requires root and
pulls in a lot of complexity):

```bash
docker run --rm -it --network host \
  -v "$PWD/../../cg-oracle-database-19c-stig-overlay:/share" \
  cincproject/auditor exec /share \
  --input-file /share/input.yml
```

The overlay's `oracledb_session` controls call `sqlplus`, so its README uses a
derived image (`cincproject/auditor` + Oracle Instant Client). See the overlay
repo's `README.md` for the authoritative image build and inputs. Local overlay
results are dev signal only.

## What local CANNOT tell you (by design)

- RDS parameter-group / option-group **effects** (moto only checks the API call is
  made; it doesn't apply anything).
- **TLS/TCPS 2484**, KMS encryption, CloudWatch log exports, GovCloud
  networking/partition, the RDS reboot/maintenance model.
- The exact RDS **privilege model** (`oracle-free` is 23c and grants more than RDS's
  master user — develop as the seeded non-SYS user to surface RDS-only failures).

All of the above is validated only on a live GovCloud RDS instance.

## Layout

```
local/
  README.md                        (this file)
  Makefile                         doctor / unit / moto-* / oracle-* / down
  docker-compose.moto.yml          motoserver/moto (free RDS control-plane mock)
  docker-compose.oracle-free.yml   gvenzl/oracle-free (native arm64)
  docker-compose.oracle-19c.yml    self-built oracle/database:19.3.0-ee (fidelity)
  scripts/
    wait-for-oracle.sh
    moto-smoke.sh                  layer-2 broker-shape smoke
    run-assessment-local.sh
    run-hardening-local.sh
  init/
    00_create_test_users.sql       non-SYS privileged app user (mirrors RDS)
    01_seed_insecure_state.sql     deliberately-weak state for detection tests
  reports/                         (gitignored) assessment/hardening output
```

The **authoritative SQL hardening scripts** live in the overlay repo
(`hardening/sql/`, kept out of the broker so the broker never runs STIG
validation itself);
this harness runs them for a fast local loop and is never wired into the broker
runtime.
