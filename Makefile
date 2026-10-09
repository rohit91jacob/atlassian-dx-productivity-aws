# Developer entrypoints. `make help` lists targets. Settings come from .env when present.
-include .env
export

UV ?= uv
TF ?= terraform
TFLINT ?= tflint
CHECKOV ?= uvx --from checkov checkov
SITE ?= site
FIXTURE_DATA ?= .fixture-run

.DEFAULT_GOAL := help
.PHONY: help install lint format test test-unit test-e2e test-aws pipeline fixtures-pipeline extract \
        curate transform dbt-athena-compile dq report verify state lambda-zip curate-requirements \
        tf-fmt tf-validate tf-lint tf-checkov infra-check precommit clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Create the virtualenv (dev + athena groups) from uv.lock
	$(UV) sync --frozen

lint: ## Ruff lint + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

format: ## Auto-fix lint and format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test-unit: ## Unit tests (no network)
	$(UV) run pytest tests/unit -q

test-e2e: ## End-to-end pipeline + idempotency tests on the recorded fixtures
	$(UV) run pytest tests/e2e -q

test-aws: ## AWS code paths on moto (in-process and server mode)
	$(UV) run pytest tests/aws -q

test: ## Everything (unit + e2e + aws/moto) with coverage
	$(UV) run pytest --cov -q

pipeline: ## Live run: extract (Jira + GitHub) -> curate -> dbt build -> mart DQ
	$(UV) run dx run

fixtures-pipeline: ## Same pipeline offline, against the recorded fixtures (what CI runs)
	DX_SOURCE=fixtures DX_DATA_DIR=$(FIXTURE_DATA) $(UV) run dx run

extract: ## Incremental extraction only
	$(UV) run dx extract

curate: ## Raw -> curated Iceberg (add ARGS=--full-refresh to rebuild)
	$(UV) run dx curate $(ARGS)

transform: ## Snapshot curated -> DuckDB and dbt build
	$(UV) run dx transform

dbt-athena-compile: ## Compile the dbt project for Athena (no AWS connection needed)
	$(UV) run dx transform --target athena --command compile

dq: ## Data-quality checks on curated + marts
	$(UV) run dx dq

report: ## Write the static results site to ./site (open site/index.html)
	$(UV) run dx report --out $(SITE)

verify: ## Reconcile outputs with published figures (live), writing verification-run.md
	$(UV) run dx verify --out verification-run.md

state: ## Show watermarks
	$(UV) run dx state

lambda-zip: ## Build dist/extract_lambda.zip
	$(UV) run python scripts/package_lambda.py

curate-requirements: ## Pin the curate image's requirements from uv.lock
	$(UV) export --no-dev --no-hashes --no-emit-project --no-group athena -q -o dist/all-requirements.txt
	$(UV) pip compile infra/lambda/requirements-curate.in -c dist/all-requirements.txt \
	  --python-platform aarch64-manylinux_2_28 --python-version 3.12 -q -o requirements-curate.txt

tf-fmt: ## terraform fmt -check
	cd infra && $(TF) fmt -check -recursive

tf-validate: ## terraform init -backend=false + validate
	cd infra && $(TF) init -backend=false -input=false >/dev/null && $(TF) validate

tf-lint: ## tflint
	cd infra && $(TFLINT) --init >/dev/null && $(TFLINT) --recursive

tf-checkov: ## checkov static analysis of infra/
	$(CHECKOV) -d infra --framework terraform --quiet --compact

infra-check: tf-fmt tf-validate tf-lint tf-checkov ## All Terraform checks

precommit: ## pre-commit on every file
	$(UV) run pre-commit run --all-files

clean: ## Remove caches and local build output (not data/)
	rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov dist dbt/target dbt/logs $(FIXTURE_DATA)
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
