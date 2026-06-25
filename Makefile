.PHONY: help install install-dev venv \
       test test-cov test-no-pyspark test-bfs test-no-label test-verbose \
       test-pyspark test-pyspark-timeout test-pyspark-basic test-pyspark-examples \
       test-pyspark-quick test-pyspark-verbose test-pyspark-features test-pyspark-fraud \
       test-pyspark-credit test-pyspark-summary \
       lint lint-fix format format-check typecheck typecheck-mypy check \
       grammar grammar-check \
       dump-sql dump-sql-save dump-sql-custom generate-golden-files \
       test-transpile test-transpile-golden diff-all \
       docs-install docs-generate-artifacts docs-generate-pages docs-generate \
       docs-serve docs-build docs-deploy docs-clean docs-full generate-readme \
       clean clean-all tree watch-test \
       build check-release version-bump-patch version-bump-minor version-bump-major \
       changelog release-dry-run release publish-test publish-dev publish

PYTHON := .venv/bin/python3
UV := uv
ANTLR_JAR := antlr-4.13.1-complete.jar
GRAMMAR_DIR := src/gsql2rsql/parser/grammar

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─────────────────────────────────────────────────────────────────────────────
# Installation
# ─────────────────────────────────────────────────────────────────────────────

install:  ## Install dependencies
	$(UV) sync

install-dev:  ## Install with dev dependencies
	$(UV) sync --extra dev
	$(UV) pip install -e ".[dev]"
venv:  ## Create virtual environment
	$(UV) venv

# ─────────────────────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────────────────────

test:  ## Run tests
	$(UV) run pytest -n 3 tests/

test-cov:  ## Run tests with coverage
	$(UV) run pytest tests/ --cov=src/gsql2rsql --cov-report=term-missing --cov-report=html

test-no-pyspark:  ## Run all tests except PySpark tests
	$(UV) run pytest tests/ -v --ignore=tests/test_examples_with_pyspark.py --ignore=tests/test_pyspark_basic.py

test-bfs:  ## Run BFS/recursive tests only
	$(UV) run pytest tests/test_renderer.py::TestBFSWithRecursive -v

test-no-label:  ## Run no-label solution tests (TDD for Solution 2.5)
	$(UV) run pytest tests/test_no_label_solution.py -v -s

test-verbose:  ## Run tests with verbose output
	$(UV) run pytest tests/ -v --tb=long

# ─────────────────────────────────────────────────────────────────────────────
# PySpark Testing
# ─────────────────────────────────────────────────────────────────────────────

test-pyspark:  ## Run all PySpark tests
	$(UV) run pytest tests/test_pyspark_basic.py tests/test_examples_with_pyspark.py -v

test-pyspark-timeout:  ## Run PySpark tests with timeout (60s per test)
	$(UV) run pytest tests/test_pyspark_basic.py tests/test_examples_with_pyspark.py -v --timeout=60 --timeout-method=thread

test-pyspark-basic:  ## Run basic PySpark infrastructure tests
	$(UV) run pytest tests/test_pyspark_basic.py -v

test-pyspark-examples:  ## Run PySpark tests on curated examples
	$(UV) run pytest tests/test_examples_with_pyspark.py -v

test-pyspark-quick:  ## Run quick PySpark validation (direct script execution)
	$(UV) run python tests/test_examples_with_pyspark.py

test-pyspark-verbose:  ## Run PySpark tests with detailed output
	$(UV) run pytest tests/test_pyspark_basic.py tests/test_examples_with_pyspark.py -v --tb=long -s

test-pyspark-features:  ## Run PySpark tests only for features_queries.yaml
	$(UV) run pytest tests/test_examples_with_pyspark.py -v -k "features_queries"

test-pyspark-fraud:  ## Run PySpark tests only for fraud_queries.yaml
	$(UV) run pytest tests/test_examples_with_pyspark.py -v -k "fraud_queries"

test-pyspark-credit:  ## Run PySpark tests only for credit_queries.yaml
	$(UV) run pytest tests/test_examples_with_pyspark.py -v -k "credit_queries"

test-pyspark-summary:  ## Generate PySpark test summary report
	$(UV) run pytest tests/test_examples_with_pyspark.py::TestExamplesSummary -v -s

# ─────────────────────────────────────────────────────────────────────────────
# Code Quality
# ─────────────────────────────────────────────────────────────────────────────

lint:  ## Run linter (ruff)
	$(UV) run ruff check src/ tests/

lint-fix:  ## Run linter and fix issues
	$(UV) run ruff check src/ tests/ --fix

format:  ## Format code (ruff)
	$(UV) run ruff format src/ tests/

format-check:  ## Check code formatting
	$(UV) run ruff format src/ tests/ --check

typecheck:  ## Run type checker (pyright)
	$(UV) run pyright src/

typecheck-mypy:  ## Run type checker (mypy)
	$(UV) run mypy src/

check: lint format-check typecheck  ## Run all checks (lint, format, typecheck)

# ─────────────────────────────────────────────────────────────────────────────
# Grammar
# ─────────────────────────────────────────────────────────────────────────────

grammar:  ## Generate ANTLR parser from grammar
	java -jar $(ANTLR_JAR) -Dlanguage=Python3 -visitor -o $(GRAMMAR_DIR) $(GRAMMAR_DIR)/Cypher.g4

grammar-check:  ## Check if ANTLR jar exists
	@test -f $(ANTLR_JAR) || (echo "Error: $(ANTLR_JAR) not found. Download from https://www.antlr.org/download.html" && exit 1)

# ─────────────────────────────────────────────────────────────────────────────
# Per-Query SQL Dump & Diff (for human validation)
# ─────────────────────────────────────────────────────────────────────────────

dump-sql:  ## Dump SQL for a specific test (usage: make dump-sql ID=01 NAME=simple_node_lookup)
	@$(UV) run python scripts/dump_query_sql.py $(ID) $(NAME) --diff

dump-sql-save:  ## Dump and save SQL to actual/ (usage: make dump-sql-save ID=01 NAME=simple_node_lookup)
	@$(UV) run python scripts/dump_query_sql.py $(ID) $(NAME) --save --diff

dump-sql-custom:  ## Dump SQL for custom Cypher (usage: make dump-sql-custom CYPHER="MATCH (n) RETURN n")
	@$(UV) run python scripts/dump_query_sql.py 00 custom --cypher "$(CYPHER)"

generate-golden-files:  ## Generate all golden SQL files for tests
	@$(UV) run python scripts/generate_all_golden_files.py

test-transpile:  ## Run transpiler tests only
	$(UV) run pytest tests/transpile_tests/ -v

test-transpile-golden:  ## Run only golden file tests
	$(UV) run pytest tests/transpile_tests/ -v -k "golden"

diff-all:  ## Show all diffs between actual and expected SQL
	@for f in tests/output/diff/*.diff; do \
		if [ -f "$$f" ]; then \
			echo "=== $$f ==="; \
			cat "$$f"; \
			echo ""; \
		fi; \
	done

# ─────────────────────────────────────────────────────────────────────────────
# Documentation
# ─────────────────────────────────────────────────────────────────────────────

docs-install:  ## Install documentation dependencies
	pip install -r requirements-docs.txt

docs-generate-artifacts:  ## Generate transpilation artifacts for examples
	-$(UV) run python examples/generate_artifacts.py

docs-generate-pages:  ## Generate documentation pages from artifacts
	$(UV) run python scripts/generate_example_docs.py

docs-generate: docs-generate-artifacts docs-generate-pages  ## Generate all documentation content

docs-serve: docs-generate  ## Serve documentation locally (generates artifacts + pages first)
	$(UV) run mkdocs serve -a 0.0.0.0:8787

docs-build:  ## Build documentation site
	$(UV) run mkdocs build

docs-deploy:  ## Deploy documentation to GitHub Pages
	$(UV) run mkdocs gh-deploy --force

docs-clean:  ## Clean documentation build artifacts
	rm -rf site/ examples/out/ docs/examples/*.md

docs-full: docs-generate docs-build  ## Generate and build documentation

generate-readme:  ## Generate README.md from docs/index.md with compiled examples
	$(UV) run python scripts/generate_readme.py

# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

clean:  ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

clean-all: clean  ## Clean everything including venv
	rm -rf .venv/

tree:  ## Show project structure
	@tree -I '__pycache__|.venv|.git|*.pyc|.pytest_cache|.mypy_cache|.ruff_cache' --dirsfirst

watch-test:  ## Run tests on file change (requires entr)
	@find src tests -name "*.py" | entr -c make test

# ─────────────────────────────────────────────────────────────────────────────
# Release & Publishing
# ─────────────────────────────────────────────────────────────────────────────

build:  ## Build package for distribution
	$(UV) build

check-release:  ## Check if package is ready for release
	@echo "Checking package..."
	$(UV) build --python $(PYTHON)
	uvx twine check dist/*

version-bump-patch:  ## Bump patch version (0.1.0 -> 0.1.1)
	$(UV) pip install python-semantic-release
	semantic-release version --patch

version-bump-minor:  ## Bump minor version (0.1.0 -> 0.2.0)
	$(UV) pip install python-semantic-release
	semantic-release version --minor

version-bump-major:  ## Bump major version (0.1.0 -> 1.0.0)
	$(UV) pip install python-semantic-release
	semantic-release version --major

changelog:  ## Generate changelog from commits
	$(UV) pip install python-semantic-release
	semantic-release changelog

release-dry-run:  ## Preview what would be released
	$(UV) pip install python-semantic-release
	semantic-release version --no-commit --no-tag --no-push

release:  ## Create a new release (CI/CD recommended)
	@echo "⚠️  Warning: This will create a new release based on commit history"
	@echo "Use 'make release-dry-run' to preview changes first"
	@read -p "Continue? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(UV) pip install python-semantic-release && \
		semantic-release version && \
		git push --follow-tags; \
	fi

publish-test:  ## Publish to TestPyPI
	$(UV) build --python $(PYTHON)
	uvx twine upload --repository testpypi dist/*

publish-dev:  ## Publish dev version to PyPI (e.g., 0.10.0.dev20260320)
	@BASE_VERSION=$$(grep '^version = ' pyproject.toml | head -1 | sed 's/version = "\(.*\)"/\1/' | sed 's/\.dev.*//') && \
	DEV_VERSION="$${BASE_VERSION}.dev$$(date +%Y%m%d%H%M)" && \
	echo "Publishing dev version: $${DEV_VERSION}" && \
	sed -i "s/^version = \".*\"/version = \"$${DEV_VERSION}\"/" pyproject.toml && \
	rm -rf dist/ && \
	($(UV) build --python $(PYTHON) && uvx twine upload dist/*; EXIT=$$?; git checkout pyproject.toml; exit $$EXIT) && \
	echo "✓ Published $${DEV_VERSION} to PyPI (pyproject.toml restored)"

publish:  ## Publish to PyPI (use GitHub Actions instead)
	@echo "⚠️  Warning: Use GitHub Actions for releases"
	@echo "Manual publish is not recommended"
	@read -p "Continue anyway? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(UV) build --python $(PYTHON) && \
		uvx twine upload dist/*; \
	fi
