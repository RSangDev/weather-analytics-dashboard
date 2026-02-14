#!/bin/bash

# Weather Analytics Dashboard - Setup Script
# Automated installation and configuration

set -e  # Exit on error

echo "🌤️  Weather Analytics Dashboard - Setup"
echo "======================================="
echo ""

# Check Python version
echo "Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
REQUIRED_VERSION="3.9"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then 
    echo "❌ Error: Python 3.9+ is required. Found: $PYTHON_VERSION"
    exit 1
fi

echo "✅ Python $PYTHON_VERSION detected"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "ℹ️  Virtual environment already exists"
fi
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✅ Virtual environment activated"
echo ""

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip > /dev/null 2>&1
echo "✅ Pip upgraded"
echo ""

# Install dependencies
echo "Installing dependencies..."
echo "This may take a few minutes..."
pip install -r requirements.txt > /dev/null 2>&1
echo "✅ Dependencies installed"
echo ""

# Create data directory
echo "Creating data directory..."
mkdir -p data
echo "✅ Data directory created"
echo ""

# Run tests
echo "Running tests..."
pytest tests/ -v --tb=short
TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ All tests passed"
else
    echo "⚠️  Some tests failed. Please check the output above."
fi
echo ""

# Setup complete
echo "======================================="
echo "✅ Setup complete!"
echo ""
echo "To start the dashboard, run:"
echo "  source venv/bin/activate"
echo "  streamlit run src/app.py"
echo ""
echo "To run tests:"
echo "  pytest tests/ -v"
echo ""
echo "To run with Airflow:"
echo "  airflow db init"
echo "  airflow webserver -p 8080 &"
echo "  airflow scheduler &"
echo ""
echo "Happy analyzing! 🌤️"