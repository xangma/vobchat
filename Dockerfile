FROM python:3.13-slim

EXPOSE 8050

# Set working directory
RUN mkdir -p /app/src
RUN mkdir -p /app/logs
WORKDIR /app/src/

# COPY . /app
# # Copy application code
# COPY src/ /app/src/
# COPY create_user.py /app/

# # Create startup script
# COPY docker-start.sh /app/docker-start.sh
# RUN chmod +x /app/docker-start.sh

# Install system dependencies
RUN apt-get update && apt-get install -y \
  gcc \
  g++ \
  libpq-dev \
  libgeos-dev \
  libproj-dev \
  libgdal-dev \
  gdal-bin \
  redis-server \
  && rm -rf /var/lib/apt/lists/*


# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Start Redis and the application
# CMD ["/app/docker-start.sh"]
CMD /bin/sh -c "redis-server --port 6379 \
                     --bind 127.0.0.1 \
                     --protected-mode no \
                     --save \"\" \
                     --stop-writes-on-bgsave-error no & \
         exec gunicorn \
                       --config /app/src/vobchat/gunicorn.conf.py \
                       vobchat.app:server"
