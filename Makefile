# ============================================================
# Booking Analytics — Common Commands
# ============================================================
# Usage:
#   make setup          Install dependencies + setup environment
#   make run            Run full pipeline locally
#   make test           Run all tests
#   make lint           Lint SQL files
#   make clean          Clean dbt artifacts
#   make all            Setup + Run + Test (full cycle)
# ============================================================

.PHONY: setup setup-minimal setup-dev run seed build test lint clean all docs help

# ----------------------------------------------------------
# Default target
# ----------------------------------------------------------
help:
	@echo ""
	@echo "Available commands:"
	@echo "  make setup          Install all dependencies"
	@echo "  make setup-minimal  Install minimal dependencies (dbt + DuckDB)"
	@echo "  make setup-dev      Install all + dev dependencies"
	@echo "  make run            Run full pipeline (seed + build + test)"
	@echo "  make seed           Load seed data into DuckDB"
	@echo "  make build          Build all dbt models"
	@echo "  make test           Run all dbt tests"
	@echo "  make lint           Lint SQL files with SQLFluff"
	@echo "  make clean          Clean dbt artifacts"
	@echo "  make docs           Generate dbt documentation"
	@echo "  make all            Full cycle: setup + run"
	@echo ""

# ----------------------------------------------------------
# Setup
# ----------------------------------------------------------

setup-dev:
	@echo "📦 Installing dev dependencies..."
	pip install -r requirements-dev.txt --ignore-installed --break-system-packages
	@echo "✅ Setup complete!"

# ----------------------------------------------------------
# dbt Commands
# ----------------------------------------------------------
seed:
	@echo "🌱 Seeding test data..."
	dbt seed --profiles-dir ./ci --target ci

build:
	@echo "🔧 Building models..."
	dbt run --profiles-dir ./ci --target ci

test:
	@echo "✅ Running tests..."
	dbt test --profiles-dir ./ci --target ci

run: seed build test
	@echo ""
	@echo "================================================"
	@echo "✅ Full pipeline completed successfully!"
	@echo "================================================"

# ----------------------------------------------------------
# Quality
# ----------------------------------------------------------
lint:
	@echo "🔍 Linting SQL files..."
	sqlfluff lint models/ --dialect trino --ignore parsing
	@echo "✅ Lint complete!"

lint-fix:
	@echo "🔧 Auto-fixing SQL files..."
	sqlfluff fix models/ --dialect trino --ignore parsing
	@echo "✅ Fix complete!"

# ----------------------------------------------------------
# Documentation
# ----------------------------------------------------------
docs:
	@echo "📚 Generating dbt docs..."
	dbt docs generate --profiles-dir ./ci --target ci
	@echo "📖 Serving docs at http://localhost:8080..."
	dbt docs serve --profiles-dir ./ci --target ci

# ----------------------------------------------------------
# Cleanup
# ----------------------------------------------------------
clean:
	@echo "🧹 Cleaning artifacts..."
	rm -rf target/
	rm -rf dbt_packages/
	rm -rf logs/
	rm -f *.duckdb
	rm -f *.duckdb.wal
	@echo "✅ Clean complete!"

# ----------------------------------------------------------
# Full Cycle
# ----------------------------------------------------------
all: setup run
	@echo ""
	@echo "================================================"
	@echo "🎉 Everything set up and validated!"
	@echo "================================================"
