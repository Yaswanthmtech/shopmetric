# Python used for the local dev venv: prefer python3.12 when it exists.
PYTHON ?= $(shell command -v python3.12 2>/dev/null || command -v python3)

.PHONY: help up down reset status logs train attack bench report venv test lint fmt contract smoke

help:            ## list targets
	@grep -E '^[a-z]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

up:              ## build + start the whole stack
	./shopmetric.sh up
down:            ## stop (keep data)
	./shopmetric.sh down
reset:           ## stop and wipe data
	./shopmetric.sh reset
status:          ## containers + latest batches
	./shopmetric.sh status
logs:            ## follow spark-stream logs
	./shopmetric.sh logs
train:           ## retrain the fraud model (hot-swapped into the running stream)
	./shopmetric.sh train
attack:          ## inject a live card-testing attack
	./shopmetric.sh attack card_testing 3
bench:           ## 3-minute load test at 200 orders/s + report
	./shopmetric.sh bench 200 3
report:          ## measured, resume-ready numbers
	./shopmetric.sh report

venv:            ## local dev environment (needs Python 3.11+ and Java 17+)
	$(PYTHON) -m venv .venv && .venv/bin/pip install -r requirements/dev.txt && .venv/bin/pip install -e .
test:            ## unit + Spark tests (Postgres e2e test runs if TEST_PG_HOST/PORT reachable)
	.venv/bin/pytest -q
lint:            ## static checks
	.venv/bin/ruff check src tests dashboard
fmt:             ## auto-format
	.venv/bin/ruff format src tests dashboard && .venv/bin/ruff check --fix src tests dashboard
contract:        ## re-export the data contract JSON Schema
	.venv/bin/python -m shopmetric.contract
smoke:           ## full Docker end-to-end smoke test (what CI runs)
	./scripts/ci_smoke.sh
