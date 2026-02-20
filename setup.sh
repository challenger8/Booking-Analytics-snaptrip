#!/bin/bash
# ============================================================
# Quick Setup Script
# Usage: bash setup.sh
# ============================================================

set -e

echo ""
echo "=================================================="
echo "  Booking Analytics — Quick Setup"
echo "=================================================="
echo ""

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "🐍 Python version: $PYTHON_VERSION"

if [[ $(echo "$PYTHON_VERSION < 3.9" | bc -l 2>/dev/null || echo "0") == "1" ]]; then
    echo "❌ Python 3.9+ required. Please upgrade."
    exit 1
fi

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements-minimal.txt -q

# Run pipeline
echo ""
echo "🌱 Seeding test data..."
dbt seed --profiles-dir ./ci --target ci

echo ""
echo "🔧 Building models..."
dbt run --profiles-dir ./ci --target ci

echo ""
echo "✅ Running tests..."
dbt test --profiles-dir ./ci --target ci

echo ""
echo "=================================================="
echo "  ✅ Setup complete! All tests passed!"
echo "=================================================="
echo ""
echo "  Activate virtual environment:"
echo "    source .venv/bin/activate"
echo ""
echo "  Useful commands:"
echo "    make run     — Run full pipeline"
echo "    make test    — Run tests only"
echo "    make lint    — Lint SQL files"
echo "    make docs    — Generate & serve documentation"
echo "    make help    — Show all commands"
echo ""
