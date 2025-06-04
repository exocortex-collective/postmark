import sqlite3
from sqlite3 import Connection


def create_connection(db_path: str) -> Connection:
    """Return a new SQLite connection to the specified database path."""
    return sqlite3.connect(db_path)


def create_db(db_path: str) -> Connection:
    """Create or upgrade the database at db_path, returning the open connection."""
    conn = create_connection(db_path)
    upgrade(conn)
    return conn


def create_schema(conn: Connection) -> None:
    """Create the emails table (only) according to the spec."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS emails(
            id INTEGER PRIMARY KEY,
            uuid TEXT UNIQUE NOT NULL,
            message_id TEXT,
            date INTEGER NOT NULL,
            subject TEXT,
            from_addr TEXT,
            schema_version INTEGER NOT NULL DEFAULT 1,
            parquet_file TEXT NOT NULL,
            parquet_row INTEGER NOT NULL
        );
        """
    )


def upgrade(conn: Connection) -> None:
    """
    Minimal migration system:
    - Create schema_version table if missing.
    - If fresh, create emails table and set schema_version to 1.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version(
            version INTEGER NOT NULL
        );
        """
    )
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    if not row:
        # Fresh DB: create emails table, set version=1
        create_schema(conn)
        conn.execute("INSERT INTO schema_version(version) VALUES (1);")
    # In a real system, you might handle subsequent versions here.
    conn.commit()
