#!/bin/bash
set -e

# Start Redis server in the background
redis-server --daemonize yes --port 6379 --bind 0.0.0.0 --protected-mode no --save "" --stop-writes-on-bgsave-error no

# Wait for Redis to be ready
echo "Waiting for Redis to start..."
until redis-cli ping; do
  sleep 1
done
echo "Redis is ready!"

# Set up environment for Python app
export PYTHONPATH="/app/src:$PYTHONPATH"

# Create logs directory if it doesn't exist
mkdir -p /app/logs

# Start the VobChat application
echo "Starting VobChat application..."
cd /app

# Initialize the database and create tables if needed
export FLASK_APP=vobchat.app
flask db upgrade 2>/dev/null || echo "Database already initialized or no migrations needed"

python -m vobchat.app