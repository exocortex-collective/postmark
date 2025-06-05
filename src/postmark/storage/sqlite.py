import sqlite3
from sqlite3 import Connection

from postmark.email.parser import Email


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
            parquet_row INTEGER NOT NULL,
            to_addrs TEXT,
            cc_addrs TEXT,
            bcc_addrs TEXT
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


def insert_email(conn: Connection, email_obj: Email, thread_id: str) -> None:
    """
    Insert an Email record into the 'emails' table.
    """
    sql = """
        INSERT INTO emails (
            uuid, message_id, date, subject, from_addr,
            to_addrs, cc_addrs, bcc_addrs, in_reply_to,
            schema_version, parquet_file, parquet_row
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'pending', -1)
    """
    date_val = 0
    if email_obj.date is not None:
        date_val = int(email_obj.date.timestamp())

    # Convert lists to comma-separated strings
    to_text = ",".join(email_obj.to_addrs or [])
    cc_text = ",".join(email_obj.cc_addrs or [])
    bcc_text = ",".join(email_obj.bcc_addrs or [])

    conn.execute(
        sql,
        (
            email_obj.uuid,
            email_obj.message_id,
            date_val,
            email_obj.subject or "",
            email_obj.from_addr or "",
            to_text,
            cc_text,
            bcc_text,
            email_obj.in_reply_to or "",
        ),
    )
    conn.commit()


def write_dead_letter(
    conn: Connection, source_file: str, offset: int, ex: Exception
) -> None:
    """
    Write a dead letter entry to 'dead_letter_queue'.
    """
    sql = """
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
          id INTEGER PRIMARY KEY,
          source_file TEXT NOT NULL,
          file_offset INTEGER NOT NULL,
          error_type TEXT NOT NULL,
          error_message TEXT,
          traceback TEXT,
          created_at INTEGER NOT NULL
        );
    """
    conn.execute(sql)
    sql2 = """
        INSERT INTO dead_letter_queue (source_file, file_offset, error_type, error_message, traceback, created_at)
        VALUES (?, ?, ?, ?, ?, strftime('%s','now'))
    """
    conn.execute(sql2, (source_file, offset, type(ex).__name__, str(ex), ""))
    conn.commit()
