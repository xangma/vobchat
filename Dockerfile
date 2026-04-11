FROM python:3.13-slim

EXPOSE 8050
EXPOSE 8000

RUN mkdir -p /app/src /app/logs /app/data
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
  gcc \
  g++ \
  libpq-dev \
  && rm -rf /var/lib/apt/lists/*

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

COPY requirements.txt pyproject.toml /app/
COPY src/ /app/src/

RUN pip install --no-cache-dir -r requirements.txt

RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["gunicorn", "--config", "/app/src/vobchat/gunicorn.conf.py", "vobchat.web.app:server"]
