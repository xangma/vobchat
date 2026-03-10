#!/bin/bash
set -e

# Start Redis
echo "Starting Redis server..."
redis-server --daemonize yes \
  --port 6379 \
  --bind 127.0.0.1 \
  --protected-mode no \
  --save "" \
  --stop-writes-on-bgsave-error no || {
    echo "Failed to start Redis server."
    exit 1
}

# Wait for Redis to be ready
echo "Waiting for Redis to start..."
for i in {1..10}; do
  if redis-cli -h 127.0.0.1 -p 6379 ping | grep -q PONG; then
    echo "Redis is ready!"
    break
  fi
  echo "Redis not ready yet... ($i/10)"
  sleep 1
done

if ! redis-cli -h 127.0.0.1 -p 6379 ping | grep -q PONG; then
  echo "Redis did not start in time."
  exit 1
fi

# Set PYTHONPATH
export PYTHONPATH="/app/src:$PYTHONPATH"

# Go to app directory
cd /app/src

# Run DB migrations if needed
export FLASK_APP=vobchat.app:server
flask db upgrade 2>/dev/null || echo "Database already initialized or no migrations needed"

# Start the app
echo "🚀 Starting VobChat..."
exec gunicorn --config /app/src/vobchat/gunicorn.conf.py vobchat.app:server
