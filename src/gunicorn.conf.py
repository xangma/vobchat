# gunicorn.conf.py
# gunicorn -c gunicorn.conf.py app:server
workers = 2  # Adjust based on CPU cores
bind = "0.0.0.0:8050"
timeout = 120  # Increase timeout to 120 seconds
graceful_timeout = 120  # Grace period for shutting down
keepalive = 5  # Keep connections open
loglevel = "debug"
preload = True