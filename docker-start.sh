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
python -m vobchat.app