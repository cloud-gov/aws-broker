# Local Oracle 19c hardening harness (WS10/11, #529).
# DEVELOPMENT SIGNAL ONLY (ADR-0005) — not compliance evidence.
#
# All targets are local Docker; nothing here touches AWS or the broker runtime.

COMPOSE_ORACLE = docker compose -f docker-compose.oracle-free.yml
COMPOSE_ORACLE19C = docker compose -f docker-compose.oracle-19c.yml
COMPOSE_MOTO = docker compose -f docker-compose.moto.yml

ORACLE_CONN ?= APPUSER/devpw_ChangeMe1@//localhost:1521/FREEPDB1

.PHONY: help
help:
	@echo "Local Oracle 19c hardening harness (development signal only):"
	@echo "  make oracle-up     start gvenzl/oracle-free (native arm64), wait healthy"
	@echo "  make oracle19c-up  start self-built Oracle 19c EE (fidelity; must build image first)"
	@echo "  make moto-up       start moto (free RDS control-plane mock) on :5000"
	@echo "  make seed          create non-SYS app user + seed insecure state"
	@echo "  make assess        run assessment SQL -> reports/ (labeled dev signal)"
	@echo "  make harden        apply allowed hardening (idempotent)"
	@echo "  make down          tear everything down"

.PHONY: oracle-up
oracle-up:
	$(COMPOSE_ORACLE) up -d
	./scripts/wait-for-oracle.sh

.PHONY: oracle19c-up
oracle19c-up:
	$(COMPOSE_ORACLE19C) up -d

.PHONY: moto-up
moto-up:
	$(COMPOSE_MOTO) up -d

.PHONY: seed
seed:
	@echo "Seeding runs automatically from init/ on first container boot."
	@echo "To re-seed, run: make down && make oracle-up"

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
