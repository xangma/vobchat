# gunicorn.conf.py
# gunicorn -c gunicorn.conf.py app:server
import trio
workers = 2  # Adjust based on CPU cores
bind = "0.0.0.0:8050"

# SSE connections require longer timeouts
timeout = 0  # Disable worker timeout for SSE connections
graceful_timeout = 300  # 5 minutes grace period for shutting down
keepalive = 30  # Keep connections open longer for SSE

# Use async worker for better SSE handling
worker_class = "gevent"  # Async worker for SSE
worker_connections = 1000  # Max concurrent connections per worker

loglevel = "debug"
preload = False

# Additional SSE-specific settings
max_requests = 0  # Don't restart workers (important for persistent SSE connections)
max_requests_jitter = 0
