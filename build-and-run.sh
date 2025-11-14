#!/bin/bash

echo "🚀 Building and starting Data Engineering Platform..."

# Build the images
echo "📦 Building Docker images..."
docker-compose build

# Start the services
echo "🔄 Starting services..."
docker-compose up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check status
echo "📊 Checking service status..."
docker-compose ps

# Show application URL
echo "✅ Data Engineering Platform is running!"
echo "🌐 Access the application at: http://localhost:8501"
echo "🔐 Admin password: admin123"
echo "📚 MySQL available at: localhost:3306"
echo ""
echo "📋 Useful commands:"
echo "   docker-compose logs -f data-engineering-app  # View application logs"
echo "   docker-compose logs -f mysql-db              # View database logs"
echo "   docker-compose down                          # Stop services"
echo "   docker-compose restart data-engineering-app  # Restart application"