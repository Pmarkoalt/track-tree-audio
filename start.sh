#!/bin/bash

# Track Tree Audio Service Startup Script

set -e

echo "Starting Track Tree Audio Service..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Warning: .env file not found. Copying from env.example..."
    cp env.example .env
    echo "Please edit .env file with your configuration before running again."
    exit 1
fi

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "Error: Redis is not running. Please start Redis first."
    echo "On macOS: brew services start redis"
    echo "On Ubuntu: sudo systemctl start redis"
    exit 1
fi

# Start Celery worker in background
echo "Starting Celery worker..."
celery -A src.queues worker --loglevel=info --detach

# Wait a moment for worker to start
sleep 2

# Start FastAPI server
echo "Starting FastAPI server..."
uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
