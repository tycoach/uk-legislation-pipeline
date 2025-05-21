FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies and PostgreSQL with its client tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql \
    postgresql-client \
    postgresql-contrib \
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    netcat-openbsd \
    sqlite3 \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Make sure PostgreSQL binaries are in the PATH
ENV PATH="/usr/lib/postgresql/13/bin:${PATH}"

# Set up PostgreSQL
RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql \
    && mkdir -p /var/lib/postgresql/data && chown -R postgres:postgres /var/lib/postgresql/data

# Create app directory
WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary requests bs4
# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY scripts/ /app/scripts/


# Create directories for data persistence
RUN mkdir -p /data/sql /data/cache /data/logs /data/checkpoints

# Make scripts executable
RUN chmod +x /app/scripts/*.sh

# Debugging - print locations of PostgreSQL binaries
RUN whereis postgresql && \
    whereis initdb && \
    whereis postgres && \
    find / -name "initdb" 2>/dev/null || echo "initdb not found"

# Set the entrypoint
ENTRYPOINT ["/app/scripts/entrypoint.sh"]

# Default command (can be overridden)
CMD ["python", "/app/src/main.py"]