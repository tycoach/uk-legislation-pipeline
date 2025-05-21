import os
import json
import logging
import time
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import Json
import sqlite3


class SQLLoader:
    """
    Loads processed legislation data into a SQL database.
    """

    def __init__(
        self,
        db_type: str = "postgresql",
        host: Optional[str] = None,
        port: Optional[int] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        sqlite_path: Optional[str] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.db_type = db_type.lower()

        if self.db_type == "postgresql":
            self.host = host or os.environ.get("DB_HOST", "localhost")
            self.port = port or int(os.environ.get("DB_PORT", 5432))
            self.dbname = dbname or os.environ.get("DB_NAME", "legislation_db")
            self.user = user or os.environ.get("DB_USER", "etl_user")
            self.password = password or os.environ.get("DB_PASSWORD", "etl_password")
            self.conn = None
        elif self.db_type == "sqlite":
            self.sqlite_path = sqlite_path or os.environ.get(
                "SQLITE_PATH", "/data/sql/legislation.db"
            )
            self.conn = None
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

        self._connect()
        self._init_tables()

    def _connect(self) -> None:
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                if self.db_type == "postgresql":
                    self.logger.info(
                        f"Connecting to PostgreSQL database {self.dbname} at {self.host}:{self.port}"
                    )
                    self.conn = psycopg2.connect(
                        host=self.host,
                        port=self.port,
                        dbname=self.dbname,
                        user=self.user,
                        password=self.password,
                    )
                    self.conn.autocommit = True
                else:
                    self.logger.info(f"Connecting to SQLite database at {self.sqlite_path}")
                    os.makedirs(os.path.dirname(self.sqlite_path), exist_ok=True)
                    self.conn = sqlite3.connect(self.sqlite_path)
                    self.conn.execute("PRAGMA foreign_keys = ON")

                self.logger.info("Database connection established successfully")
                break
            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}"
                )
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    self.logger.error("Failed to connect to database after retries")
                    raise

    def _init_tables(self) -> None:
        if not self.conn:
            self.logger.error("No database connection available")
            return

        try:
            cursor = self.conn.cursor()

            if self.db_type == "postgresql":
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_legislation_id ON legislation(legislation_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_legislation_id ON legislation_sections(legislation_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_legislation_id ON legislation_embeddings(legislation_id)")
            else:
                # SQLite schema
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute(
                    """
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
                    """
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_legislation_id ON legislation(legislation_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sections_legislation_id ON legislation_sections(legislation_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_embeddings_legislation_id ON legislation_embeddings(legislation_id)")

            self.conn.commit()
            self.logger.info("Database tables initialized successfully")

        except Exception as e:
            self.logger.error(f"Error initializing database tables: {str(e)}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    def store_legislation(self, legislation_data: Dict[str, Any]) -> bool:
        if not self.conn:
            self._connect()

        if not legislation_data or "id" not in legislation_data:
            self.logger.error("Invalid legislation data: missing ID")
            return False

        legislation_id = legislation_data["id"]
        cursor = None

        try:
            cursor = self.conn.cursor()

            # Insert or update legislation main record
            if self.db_type == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO legislation
                    (legislation_id, title, url, year, doc_type, number, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (legislation_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        year = EXCLUDED.year,
                        doc_type = EXCLUDED.doc_type,
                        number = EXCLUDED.number,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        legislation_id,
                        legislation_data.get("title", ""),
                        legislation_data.get("url", ""),
                        legislation_data.get("year", ""),
                        legislation_data.get("type", ""),
                        legislation_data.get("number", ""),
                        Json(legislation_data.get("metadata", {})),
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO legislation
                    (legislation_id, title, url, year, doc_type, number, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        legislation_id,
                        legislation_data.get("title", ""),
                        legislation_data.get("url", ""),
                        legislation_data.get("year", ""),
                        legislation_data.get("type", ""),
                        legislation_data.get("number", ""),
                        json.dumps(legislation_data.get("metadata", {})),
                    ),
                )

            # Store sections
            if "content" in legislation_data and legislation_data["content"]:
                cursor.execute(
                    "DELETE FROM legislation_sections WHERE legislation_id = %s"
                    if self.db_type == "postgresql"
                    else "DELETE FROM legislation_sections WHERE legislation_id = ?",
                    (legislation_id,),
                )

                for idx, section in enumerate(legislation_data["content"]):
                    if self.db_type == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO legislation_sections
                            (legislation_id, section_idx, section_type, section_number, section_title, text)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                legislation_id,
                                idx,
                                section.get("section_type", ""),
                                section.get("section_number", ""),
                                section.get("section_title", ""),
                                section.get("text", ""),
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO legislation_sections
                            (legislation_id, section_idx, section_type, section_number, section_title, text)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                legislation_id,
                                idx,
                                section.get("section_type", ""),
                                section.get("section_number", ""),
                                section.get("section_title", ""),
                                section.get("text", ""),
                            ),
                        )

            # Store embedding references (not the vectors)
            if "embeddings" in legislation_data and legislation_data["embeddings"]:
                cursor.execute(
                    "DELETE FROM legislation_embeddings WHERE legislation_id = %s"
                    if self.db_type == "postgresql"
                    else "DELETE FROM legislation_embeddings WHERE legislation_id = ?",
                    (legislation_id,),
                )

                for embedding_data in legislation_data["embeddings"]:
                    embedding_id = f"{legislation_id}_s{embedding_data.get('section_idx', 0)}_c{embedding_data.get('chunk_idx', 0)}"
                    if self.db_type == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO legislation_embeddings
                            (legislation_id, section_idx, chunk_idx, text, embedding_id)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                legislation_id,
                                embedding_data.get("section_idx", 0),
                                embedding_data.get("chunk_idx", 0),
                                embedding_data.get("text", ""),
                                embedding_id,
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO legislation_embeddings
                            (legislation_id, section_idx, chunk_idx, text, embedding_id)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                legislation_id,
                                embedding_data.get("section_idx", 0),
                                embedding_data.get("chunk_idx", 0),
                                embedding_data.get("text", ""),
                                embedding_id,
                            ),
                        )

            self.conn.commit()
            self.logger.info(f"Stored legislation: {legislation_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error storing legislation {legislation_id}: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def batch_store_legislation(self, legislation_list: List[Dict[str, Any]]) -> int:
        success_count = 0
        for legislation in legislation_list:
            if self.store_legislation(legislation):
                success_count += 1
        self.logger.info(f"Stored {success_count}/{len(legislation_list)} legislation documents")
        return success_count

    def get_legislation_by_id(self, legislation_id: str) -> Optional[Dict[str, Any]]:
        if not self.conn:
            self._connect()
        cursor = None
        try:
            cursor = self.conn.cursor()
            if self.db_type == "postgresql":
                cursor.execute("SELECT * FROM legislation WHERE legislation_id = %s", (legislation_id,))
            else:
                cursor.execute("SELECT * FROM legislation WHERE legislation_id = ?", (legislation_id,))

            result = cursor.fetchone()
            if not result:
                return None

            columns = [desc[0] for desc in cursor.description]
            legislation = dict(zip(columns, result))

            if self.db_type == "sqlite" and "metadata" in legislation and legislation["metadata"]:
                legislation["metadata"] = json.loads(legislation["metadata"])

            if self.db_type == "postgresql":
                cursor.execute("SELECT * FROM legislation_sections WHERE legislation_id = %s ORDER BY section_idx", (legislation_id,))
            else:
                cursor.execute("SELECT * FROM legislation_sections WHERE legislation_id = ? ORDER BY section_idx", (legislation_id,))

            sections = []
            for row in cursor.fetchall():
                section_columns = [desc[0] for desc in cursor.description]
                section = dict(zip(section_columns, row))
                sections.append(section)

            legislation["content"] = sections

            if self.db_type == "postgresql":
                cursor.execute("SELECT * FROM legislation_embeddings WHERE legislation_id = %s ORDER BY section_idx, chunk_idx", (legislation_id,))
            else:
                cursor.execute("SELECT * FROM legislation_embeddings WHERE legislation_id = ? ORDER BY section_idx, chunk_idx", (legislation_id,))

            embeddings = []
            for row in cursor.fetchall():
                embedding_columns = [desc[0] for desc in cursor.description]
                embedding = dict(zip(embedding_columns, row))
                embeddings.append(embedding)

            legislation["embedding_refs"] = embeddings

            return legislation
        except Exception as e:
            self.logger.error(f"Error retrieving legislation {legislation_id}: {str(e)}")
            return None
        finally:
            if cursor:
                cursor.close()

    def get_embedding_info(self, embedding_ids: List[str]) -> List[Dict[str, Any]]:
        if not self.conn or not embedding_ids:
            return []
        cursor = None
        try:
            cursor = self.conn.cursor()
            result = []
            for embedding_id in embedding_ids:
                if self.db_type == "postgresql":
                    cursor.execute(
                        """
                        SELECT e.*, l.title, s.section_title 
                        FROM legislation_embeddings e
                        JOIN legislation l ON e.legislation_id = l.legislation_id
                        JOIN legislation_sections s ON e.legislation_id = s.legislation_id AND e.section_idx = s.section_idx
                        WHERE e.embedding_id = %s
                        """,
                        (embedding_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT e.*, l.title, s.section_title 
                        FROM legislation_embeddings e
                        JOIN legislation l ON e.legislation_id = l.legislation_id
                        JOIN legislation_sections s ON e.legislation_id = s.legislation_id AND e.section_idx = s.section_idx
                        WHERE e.embedding_id = ?
                        """,
                        (embedding_id,),
                    )
                row = cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    embedding_info = dict(zip(columns, row))
                    result.append(embedding_info)
            return result
        except Exception as e:
            self.logger.error(f"Error retrieving embedding info: {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")
