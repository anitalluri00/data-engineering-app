#!/bin/bash

echo "🚀 Setting up Data Engineering Platform..."

# Create directory structure
echo "📁 Creating directory structure..."
mkdir -p src/database src/ingestion src/processing src/analytics src/utils
mkdir -p data/raw data/processed data/reports logs models config

# Create required files
echo "📄 Creating required files..."
touch requirements.txt .env init.sql config/logging.conf

# Create basic Python files if they don't exist
if [ ! -f "src/app.py" ]; then
    echo "# Main application" > src/app.py
fi

if [ ! -f "src/database/__init__.py" ]; then
    touch src/database/__init__.py
    touch src/ingestion/__init__.py
    touch src/processing/__init__.py
    touch src/analytics/__init__.py
    touch src/utils/__init__.py
fi

echo "✅ Project structure created!"
echo "📋 Next steps:"
echo "   1. Add your Python source files to src/"
echo "   2. Run: docker compose build --no-cache"
echo "   3. Run: docker compose up -d"
echo "   4. Access at: http://localhost:8501"
