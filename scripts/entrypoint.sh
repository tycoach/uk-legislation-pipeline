#!/bin/bash
set -e

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Find PostgreSQL binaries
find_pg_binaries() {
    log "Searching for PostgreSQL binaries..."
    
    # Find PostgreSQL version and set binary path
    PG_VERSION=$(find /usr/lib/postgresql/ -maxdepth 1 -type d | grep -oP '(?<=/usr/lib/postgresql/)[0-9]+' | sort -nr | head -n 1)
    
    if [ -n "$PG_VERSION" ]; then
        log "Found PostgreSQL version: $PG_VERSION"
        PG_BINDIR="/usr/lib/postgresql/$PG_VERSION/bin"
        export PATH="$PG_BINDIR:$PATH"
        log "Added $PG_BINDIR to PATH"
    else
        log "Could not find PostgreSQL version. Checking for binaries directly..."
        
        # Try to find initdb directly
        INITDB_PATH=$(find / -name "initdb" -type f -executable 2>/dev/null | head -n 1)
        
        if [ -n "$INITDB_PATH" ]; then
            PG_BINDIR=$(dirname "$INITDB_PATH")
            export PATH="$PG_BINDIR:$PATH"
            log "Added $PG_BINDIR to PATH"
        else
            log "WARNING: Could not find PostgreSQL binaries. Will try to proceed anyway."
        fi
    fi
    
    # Check if we can execute PostgreSQL commands
    if command -v initdb >/dev/null 2>&1; then
        log "initdb is available at: $(which initdb)"
    else 
        log "ERROR: initdb command not found in PATH"
    fi
    
    if command -v pg_ctl >/dev/null 2>&1; then
        log "pg_ctl is available at: $(which pg_ctl)"
    else
        log "ERROR: pg_ctl command not found in PATH"
    fi
}

# Start PostgreSQL
start_postgres() {
    log "Starting PostgreSQL database..."
    
    # Try to use external PostgreSQL
    if [ "$USE_EXTERNAL_PG" = "true" ]; then
        log "Using external PostgreSQL server at $DB_HOST:$DB_PORT"
        
        # Test connection to external PostgreSQL
        for i in {1..30}; do
            export PGPASSWORD="$DB_PASSWORD"
            if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c '\q' >/dev/null 2>&1; then
                log "Connected to external PostgreSQL server"
                return 0
            fi
            log "Waiting for external PostgreSQL to be ready... ($i/30)"
            sleep 1
        done
        
        log "ERROR: Could not connect to external PostgreSQL server"
        return 1
    fi
    
    # Check if PostgreSQL is already running
    if command -v pg_isready >/dev/null 2>&1 && pg_isready -q; then
        log "PostgreSQL is already running"
        return 0
    fi
    
    # Data directory
    PGDATA=${PGDATA:-/var/lib/postgresql/data}
    
    # Initialize PostgreSQL if data directory is empty
    if [ -z "$(ls -A $PGDATA 2>/dev/null)" ]; then
        log "Initializing PostgreSQL data directory..."
        
        # Create and set permissions on data directory
        mkdir -p $PGDATA
        chown -R postgres:postgres $PGDATA
        chmod 700 $PGDATA
        
        # Initialize the database
        if command -v initdb >/dev/null 2>&1; then
            su postgres -c "initdb -D $PGDATA"
            
            # Configure PostgreSQL to listen on all interfaces
            echo "listen_addresses = '*'" >> $PGDATA/postgresql.conf
            echo "host all all 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
        else
            log "ERROR: initdb command not found. Cannot initialize PostgreSQL database."
            log "Will try to continue in case PostgreSQL is managed externally."
        fi
    fi
    
    # Start PostgreSQL
    if command -v pg_ctl >/dev/null 2>&1; then
        log "Starting PostgreSQL service..."
        mkdir -p /data/logs
        su postgres -c "pg_ctl -D $PGDATA -l /data/logs/postgresql.log start"
        
        # Wait for PostgreSQL to start
        for i in {1..30}; do
            if pg_isready -q; then
                log "PostgreSQL started successfully"
                return 0
            fi
            log "Waiting for PostgreSQL to start... ($i/30)"
            sleep 1
        done
        
        log "ERROR: PostgreSQL failed to start"
    else
        log "ERROR: pg_ctl command not found. Cannot start PostgreSQL."
        log "Will try to continue in case PostgreSQL is managed externally."
    fi
    
    # If we get here, try to see if PostgreSQL is accessible anyway
    if command -v psql >/dev/null 2>&1; then
        if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c '\q' >/dev/null 2>&1; then
            log "Connected to PostgreSQL server at $DB_HOST:$DB_PORT"
            return 0
        fi
    fi
    
    log "WARNING: Could not connect to any PostgreSQL server. The application may fail."
    return 1
}

# Setup database user and permissions
setup_database_user() {
    log "Setting up database user and database..."
    
    # Get database connection info from environment
    DB_HOST=${DB_HOST:-localhost}
    DB_PORT=${DB_PORT:-5432}
    DB_USER=${DB_USER:-postgres}
    DB_PASSWORD=${DB_PASSWORD:-postgres}
    DB_NAME=${DB_NAME:-legislation_db}
    
    log "Database configuration: DB_HOST=$DB_HOST, DB_PORT=$DB_PORT, DB_USER=$DB_USER, DB_NAME=$DB_NAME"
    
    # Check if we can run PostgreSQL commands
    if ! command -v psql >/dev/null 2>&1; then
        log "ERROR: psql command not found. Cannot set up database."
        log "Will try to continue assuming the database is already set up."
        return 1
    fi
    
    # If using local PostgreSQL
    if [ "$DB_HOST" = "localhost" ] || [ "$DB_HOST" = "127.0.0.1" ]; then
        log "Using local PostgreSQL server"
        
        # Check if user exists
        if su postgres -c "psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'\"" | grep -q 1; then
            log "User $DB_USER already exists"
        else
            # Create user
            su postgres -c "psql -c \"CREATE ROLE $DB_USER WITH LOGIN SUPERUSER PASSWORD '$DB_PASSWORD';\""
            log "Created user $DB_USER"
        fi
        
        # Check if database exists
        if su postgres -c "psql -tAc \"SELECT 1 FROM pg_database WHERE datname='$DB_NAME'\"" | grep -q 1; then
            log "Database $DB_NAME already exists"
        else
            # Create database
            su postgres -c "psql -c \"CREATE DATABASE $DB_NAME OWNER $DB_USER;\""
            log "Created database $DB_NAME owned by $DB_USER"
        fi
    else
        log "Using external PostgreSQL server at $DB_HOST"
        
        # Test connection to external PostgreSQL
        if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c '\q' >/dev/null 2>&1; then
            log "Connected to external PostgreSQL server"
            
            # Check if database exists
            if PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1; then
                log "Database $DB_NAME already exists on external server"
            else
                # Create database
                PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME;"
                log "Created database $DB_NAME on external server"
            fi
        else
            log "ERROR: Could not connect to external PostgreSQL server"
            return 1
        fi
    fi
    
    log "Database setup completed"
    return 0
}

# Create required directories
create_directories() {
    log "Creating required directories..."
    
    # Create data directories
    mkdir -p /data/cache
    mkdir -p /data/checkpoints
    mkdir -p /data/logs
    mkdir -p /data/sql
    
    # Set proper permissions
    chmod -R 777 /data
    
    log "Directories created"
}

# Wait for Qdrant to be ready
wait_for_qdrant() {
    log "Waiting for Qdrant to be ready..."
    
    # Use environment variables without fallbacks
    VECTOR_DB_HOST=${VECTOR_DB_HOST:-qdrant}
    VECTOR_DB_PORT=${VECTOR_DB_PORT:-6333}
    
    log "Vector DB configuration: VECTOR_DB_HOST=$VECTOR_DB_HOST, VECTOR_DB_PORT=$VECTOR_DB_PORT"
    
    for i in {1..30}; do
        if nc -z $VECTOR_DB_HOST $VECTOR_DB_PORT >/dev/null 2>&1; then
            log "Qdrant is ready at $VECTOR_DB_HOST:$VECTOR_DB_PORT"
            return 0
        fi
        log "Waiting for Qdrant to be ready at $VECTOR_DB_HOST:$VECTOR_DB_PORT... ($i/30)"
        sleep 1
    done
    
    log "ERROR: Qdrant is not ready after 30 seconds"
    return 1
}

# Print environment variables (for debugging)
print_env_vars() {
    log "Environment variables:"
    log "DB_HOST=$DB_HOST"
    log "DB_PORT=$DB_PORT"
    log "DB_USER=$DB_USER"
    log "DB_NAME=$DB_NAME"
    log "VECTOR_DB_HOST=$VECTOR_DB_HOST"
    log "VECTOR_DB_PORT=$VECTOR_DB_PORT"
    log "LEGISLATION_TIME_PERIOD=$LEGISLATION_TIME_PERIOD"
    log "LEGISLATION_CATEGORY=$LEGISLATION_CATEGORY"
}

# Main execution starts here
log "Starting UK Legislation ETL Pipeline container"

# Print environment variables for debugging
print_env_vars

# Create directories
create_directories

# Find PostgreSQL binaries
find_pg_binaries

# Initialize PostgreSQL
start_postgres

# Setup database with the specified credentials
setup_database_user

# Wait for Qdrant (external service)
wait_for_qdrant

# Check command - if it's for querying, run query.py
if [[ "$1" == "python" && "$2" == *"query.py"* ]]; then
    log "Running query command: $@"
    exec "$@"
elif [[ "$1" == "query" ]]; then
    # Handle 'query' command as a shortcut to query.py
    shift
    log "Running query: $@"
    exec python /app/src/query.py "$@"
else
    # Default to running the ETL pipeline
    log "Running ETL pipeline"
    python /app/src/main.py
    
    # Keep container running
    log "ETL pipeline completed, keeping container running for queries"
    log "Use 'docker exec <container> python /app/src/query.py \"your search query\"' to search"
    tail -f /dev/null
fi