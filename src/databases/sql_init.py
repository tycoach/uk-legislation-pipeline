import os
import sys
import time
import logging
import subprocess
from typing import Dict, Any, Optional
import psycopg2
import sqlite3


def init_sql_database(
    db_type: str = "postgresql",
    host: str = None,
    port: int = None,
    dbname: str = None,
    user: str = None,
    password: str = None,
    sqlite_path: str = None,
    init_tables: bool = True
) -> bool:
    """
    Initialize SQL database for the ETL pipeline.
    
    This function:
    1. Checks if the database server is running (PostgreSQL only)
    2. Creates the database if it doesn't exist
    3. Creates the necessary tables
    
    Args:
        db_type: Type of database ('postgresql' or 'sqlite')
        host: Database host (PostgreSQL only)
        port: Database port (PostgreSQL only)
        dbname: Database name (PostgreSQL only)
        user: Database user (PostgreSQL only)
        password: Database password (PostgreSQL only)
        sqlite_path: Path to SQLite database file (SQLite only)
        init_tables: Whether to initialize tables
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    # Set connection parameters from environment variables if not provided
    if db_type.lower() == 'postgresql':
        host = host or os.environ.get('DB_HOST', '')
        port = port or int(os.environ.get('DB_PORT'))
        dbname = dbname or os.environ.get('DB_NAME', 'legislation_db')
        user = user or os.environ.get('DB_USER')
        password = password or os.environ.get('DB_PASSWORD')
    elif db_type.lower() == 'sqlite':
        sqlite_path = sqlite_path or os.environ.get('SQLITE_PATH', '/data/sql/legislation.db')
    else:
        logger.error(f"Unsupported database type: {db_type}")
        return False
    
    logger.info(f"Initializing {db_type} database")
    
    try:
        if db_type.lower() == 'postgresql':
            return _init_postgresql(host, port, dbname, user, password, init_tables)
        else:  # sqlite
            return _init_sqlite(sqlite_path, init_tables)
            
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False


def _init_postgresql(
    host: str, 
    port: int, 
    dbname: str, 
    user: str, 
    password: str,
    init_tables: bool
) -> bool:
    """
    Initialize PostgreSQL database.
    
    Args:
        host: Database host
        port: Database port
        dbname: Database name
        user: Database user
        password: Database password
        init_tables: Whether to initialize tables
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    # Check if PostgreSQL is running
    if not _is_postgresql_running(host, port):
        logger.info("PostgreSQL is not running. Attempting to start...")
        if not _start_postgresql():
            logger.error("Failed to start PostgreSQL")
            return False
        
        # Wait for PostgreSQL to start
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            if _is_postgresql_running(host, port):
                logger.info("PostgreSQL started successfully")
                break
                
            logger.info(f"Waiting for PostgreSQL to start (attempt {attempt+1}/{max_retries})...")
            time.sleep(retry_delay)
            retry_delay *= 1.5  # Exponential backoff
        else:
            logger.error("PostgreSQL did not start in time")
            return False
    
    # Connect to PostgreSQL server 
    try:
        # First connect to 'postgres' database to be able to create our database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname="postgres",
            user=user,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        if cursor.fetchone() is None:
            # Create database
            logger.info(f"Creating database: {dbname}")
            cursor.execute(f"CREATE DATABASE {dbname}")
        else:
            logger.info(f"Database already exists: {dbname}")
            
        # Close connection to postgres database
        cursor.close()
        conn.close()
        
        # Connect to the target database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        if init_tables:
            # Create tables
            logger.info("Creating tables")
            
            # Create legislation table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS legislation (
                    id SERIAL PRIMARY KEY,
                    legislation_id VARCHAR(255) UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT,
                    year VARCHAR(50),
                    doc_type VARCHAR(100),
                    number VARCHAR(100),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create legislation_sections table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS legislation_sections (
                    id SERIAL PRIMARY KEY,
                    legislation_id VARCHAR(255) NOT NULL,
                    section_idx INTEGER NOT NULL,
                    section_type VARCHAR(100),
                    section_number VARCHAR(100),
                    section_title TEXT,
                    text TEXT NOT NULL,
                    FOREIGN KEY (legislation_id) REFERENCES legislation(legislation_id) ON DELETE CASCADE,
                    UNIQUE (legislation_id, section_idx)
                )
            """)
            
            # Create legislation_embeddings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS legislation_embeddings (
                    id SERIAL PRIMARY KEY,
                    legislation_id VARCHAR(255) NOT NULL,
                    section_idx INTEGER NOT NULL,
                    chunk_idx INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    embedding_id VARCHAR(255) UNIQUE NOT NULL,
                    FOREIGN KEY (legislation_id) REFERENCES legislation(legislation_id) ON DELETE CASCADE,
                    UNIQUE (legislation_id, section_idx, chunk_idx)
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_legislation_id ON legislation(legislation_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_legislation_id ON legislation_sections(legislation_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_legislation_id ON legislation_embeddings(legislation_id)")
            
            logger.info("Tables created successfully")
        
        # Close connection
        cursor.close()
        conn.close()
        
        logger.info("PostgreSQL database initialization complete")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing PostgreSQL database: {str(e)}")
        return False


def _init_sqlite(sqlite_path: str, init_tables: bool) -> bool:
    """
    Initialize SQLite database.
    
    Args:
        sqlite_path: Path to SQLite database file
        init_tables: Whether to initialize tables
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        
        # Connect to database (creates it if it doesn't exist)
        conn = sqlite3.connect(sqlite_path)
        
        if init_tables:
            cursor = conn.cursor()
            
            # Enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")
            
            # Create legislation table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS legislation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    legislation_id TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT,
                    year TEXT,
                    doc_type TEXT,
                    number TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create legislation_sections table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS legislation_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    legislation_id TEXT NOT NULL,
                    section_idx INTEGER NOT NULL,
                    section_type TEXT,
                    section_number TEXT,
                    section_title TEXT,
                    text TEXT NOT NULL,
                    FOREIGN KEY (legislation_id) REFERENCES legislation(legislation_id) ON DELETE CASCADE,
                    UNIQUE (legislation_id, section_idx)
                )
            """)
            
            # Create legislation_embeddings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS legislation_embeddings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    legislation_id TEXT NOT NULL,
                    section_idx INTEGER NOT NULL,
                    chunk_idx INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    embedding_id TEXT UNIQUE NOT NULL,
                    FOREIGN KEY (legislation_id) REFERENCES legislation(legislation_id) ON DELETE CASCADE,
                    UNIQUE (legislation_id, section_idx, chunk_idx)
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_legislation_id ON legislation(legislation_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_legislation_id ON legislation_sections(legislation_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_legislation_id ON legislation_embeddings(legislation_id)")
            
            # Commit changes
            conn.commit()
            
            logger.info("SQLite tables created successfully")
        
        # Close connection
        conn.close()
        
        logger.info("SQLite database initialization complete")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing SQLite database: {str(e)}")
        return False


def _is_postgresql_running(host: str, port: int) -> bool:
    """
    Check if PostgreSQL server is running.
    
    Args:
        host: Database host
        port: Database port
        
    Returns:
        True if running, False otherwise
    """
    try:
        # Try to connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname="legislation_db",
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            connect_timeout=3
        )
        conn.close()
        return True
    except:
        return False


def _start_postgresql() -> bool:
    """
    Start PostgreSQL server.
    
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Check if we're in a Docker container
        in_docker = os.path.exists('/.dockerenv')
        
        if in_docker:
            # In Docker, use service command
            subprocess.run(["service", "postgresql", "start"], check=True)
        else:
            # On local machine, try pg_ctl
            if sys.platform.startswith('linux'):
                subprocess.run(["sudo", "systemctl", "start", "postgresql"], check=True)
            elif sys.platform == 'darwin':  # macOS
                subprocess.run(["pg_ctl", "-D", "/usr/local/var/postgres", "start"], check=True)
            else:
                logger.error(f"Unsupported platform for automatic PostgreSQL start: {sys.platform}")
                return False
                
        return True
    except subprocess.SubprocessError as e:
        logger.error(f"Failed to start PostgreSQL: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error starting PostgreSQL: {str(e)}")
        return False


if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize database with parameters from environment variables
    db_type = os.environ.get('DB_TYPE', 'postgresql')
    
    if init_sql_database(db_type=db_type):
        print("Database initialization successful")
        sys.exit(0)
    else:
        print("Database initialization failed")
        sys.exit(1)