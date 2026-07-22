# Local Oracle 19c test harness.
# DEVELOPMENT SIGNAL ONLY — not compliance evidence.
#
# All targets are local Docker + go test; nothing here touches AWS or production.
# New here? Run `make -C local doctor` then `make -C local quickstart`.

COMPOSE_ORACLE = docker compose -f docker-compose.oracle-free.yml
COMPOSE_ORACLE19C = docker compose -f docker-compose.oracle-19c.yml
COMPOSE_MOTO = docker compose -f docker-compose.moto.yml

ORACLE_CONN ?= APPUSER/devpw_ChangeMe1@//localhost:1521/FREEPDB1
# Repo root (this Makefile lives in local/).
ROOT = ..
# The suite pulls in go-sqlite3 (a cgo package), so unit tests need cgo + a C
# compiler. Force it on here rather than relying on the toolchain default.
export CGO_ENABLED = 1

.PHONY: help
help: quickstart

.PHONY: quickstart
quickstart:
	@echo "Local Oracle 19c test harness — 3 layers (development signal only):"
	@echo ""
	@echo "  0. make doctor        check your machine has the prerequisites"
	@echo ""
	@echo "  1. UNIT TESTS (fast; no Docker) — the everyday loop:"
	@echo "     make unit           go test ./... with test secrets/catalog wired"
	@echo ""
	@echo "  2. BROKER FLOW vs a mock AWS RDS control plane (moto):"
	@echo "     make moto-up        start moto on :5000"
	@echo "     make moto-smoke     create an Oracle instance/param+option group via the AWS API"
	@echo "     make moto-down      stop moto"
	@echo ""
	@echo "  3. REAL LOCAL ORACLE for SQL hardening/assessment:"
	@echo "     make oracle-up      start gvenzl/oracle-free (native arm64), wait healthy"
	@echo "     make assess         run assessment SQL -> reports/ (labeled dev signal)"
	@echo "     make harden         apply allowed hardening (idempotent)"
	@echo "     make assess         re-assess: state should have changed"
	@echo "     make oracle19c-up   (optional) self-built Oracle 19c EE fidelity image"
	@echo ""
	@echo "  make down             tear EVERYTHING down"
	@echo ""
	@echo "See README.md for the full from-a-clean-laptop guide."

# ---------------------------------------------------------------------------
# 0. Prerequisite check
# ---------------------------------------------------------------------------
.PHONY: doctor
doctor:
	@echo "== Local test harness prerequisites (macOS arm64 / Linux) =="
	@ok=1; \
	printf "%-22s" "docker:"; \
	  if command -v docker >/dev/null 2>&1; then \
	    if docker info >/dev/null 2>&1; then echo "OK (daemon running)"; \
	    else echo "INSTALLED but daemon NOT running — start Docker Desktop/Colima"; ok=0; fi; \
	  else echo "MISSING — install Docker Desktop or 'brew install colima docker && colima start'"; ok=0; fi; \
	printf "%-22s" "go:"; \
	  if command -v go >/dev/null 2>&1; then go version | awk '{print "OK ("$$3")"}'; \
	  else echo "MISSING — 'brew install go' (needed for layer 1 unit tests)"; ok=0; fi; \
	printf "%-22s" "C compiler (cgo):"; \
	  if command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1 || command -v clang >/dev/null 2>&1; then echo "OK (cgo builds go-sqlite3)"; \
	  else echo "MISSING — layer 1 needs cgo: 'xcode-select --install' (macOS) or 'apt-get install -y gcc' (Linux)"; ok=0; fi; \
	printf "%-22s" "aws (cli):"; \
	  if command -v aws >/dev/null 2>&1; then echo "OK (layer 2 moto smoke)"; \
	  else echo "MISSING for layer 2 — 'brew install awscli' (macOS) or 'apt-get install -y awscli' (Linux); only 'make moto-smoke' needs it"; fi; \
	printf "%-22s" "cinc-auditor:"; \
	  if command -v cinc-auditor >/dev/null 2>&1; then echo "OK (overlay InSpec run)"; \
	  else echo "optional — 'brew install --cask cinc-auditor' (only to run the STIG overlay locally)"; fi; \
	printf "%-22s" "sqlplus:"; \
	  if command -v sqlplus >/dev/null 2>&1; then echo "OK"; \
	  else echo "not needed — 'make assess/harden' run sqlplus INSIDE the container"; fi; \
	printf "%-22s" "overlay repo:"; \
	  if [ -d ../../cg-oracle-database-19c-stig-overlay/hardening/sql ]; then echo "OK (sibling clone found)"; \
	  else echo "MISSING for layer 3 — clone cg-oracle-database-19c-stig-overlay as a sibling of aws-broker (or set SQL_DIR=)"; fi; \
	echo ""; \
	if [ "$$ok" = "1" ]; then echo "Ready for layers 1-2. Layer 3 also needs the overlay sibling clone (see above)."; \
	else echo "Install the MISSING items above, then re-run 'make doctor'."; exit 1; fi

# ---------------------------------------------------------------------------
# 1. Unit tests (no Docker). Wires the test secrets/catalog the suite needs.
# ---------------------------------------------------------------------------
.PHONY: unit
unit:
	@echo "Wiring test config (secrets-test.yml/catalog-test.yml -> secrets.yml/catalog.yml)..."
	cp $(ROOT)/secrets-test.yml $(ROOT)/secrets.yml
	cp $(ROOT)/catalog-test.yml $(ROOT)/catalog.yml
	cp $(ROOT)/secrets-test.yml $(ROOT)/cmd/tasks/secrets.yml
	cp $(ROOT)/catalog-test.yml $(ROOT)/cmd/tasks/catalog.yml
	cd $(ROOT) && go test ./...
	cd $(ROOT)/cmd/tasks && go test ./...
	@echo "unit tests passed (secrets.yml/catalog.yml are gitignored, left in place)."

.PHONY: unit-oracle
unit-oracle:
	cp $(ROOT)/secrets-test.yml $(ROOT)/secrets.yml
	cp $(ROOT)/catalog-test.yml $(ROOT)/catalog.yml
	cd $(ROOT) && go test ./services/rds/... -run Oracle -v

# ---------------------------------------------------------------------------
# 2. Broker control-flow vs moto (mock AWS RDS control plane)
# ---------------------------------------------------------------------------
.PHONY: moto-up
moto-up:
	$(COMPOSE_MOTO) up -d
	@echo "moto up on http://localhost:5000 (point the AWS SDK/CLI at it via --endpoint-url / BaseEndpoint)."

.PHONY: moto-smoke
moto-smoke:
	./scripts/moto-smoke.sh

.PHONY: moto-down
moto-down:
	-$(COMPOSE_MOTO) down -v

# ---------------------------------------------------------------------------
# 3. Real local Oracle (gvenzl/oracle-free, native arm64) for SQL hardening
# ---------------------------------------------------------------------------
.PHONY: oracle-up
oracle-up:
	$(COMPOSE_ORACLE) up -d
	./scripts/wait-for-oracle.sh

.PHONY: oracle19c-up
oracle19c-up:
	$(COMPOSE_ORACLE19C) up -d

.PHONY: assess
assess:
	./scripts/run-assessment-local.sh "$(ORACLE_CONN)"

.PHONY: harden
harden:
	./scripts/run-hardening-local.sh "$(ORACLE_CONN)"

.PHONY: down
down:
	-$(COMPOSE_ORACLE) down -v
	-$(COMPOSE_ORACLE19C) down -v
	-$(COMPOSE_MOTO) down -v
