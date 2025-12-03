import sqlite3
import traceback
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
    """Create the emails and attachments tables according to the spec."""
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
            to_addrs TEXT,
            cc_addrs TEXT,
            bcc_addrs TEXT,
            in_reply_to TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS attachments(
            id INTEGER PRIMARY KEY,
            email_id INTEGER NOT NULL,
            storage_service TEXT NOT NULL,
            storage_address TEXT NOT NULL,
            attachment_number INTEGER NOT NULL,
            attachment_name TEXT NOT NULL,
            mime_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(email_id) REFERENCES emails(id),
            UNIQUE(email_id, attachment_number)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
          id INTEGER PRIMARY KEY,
          source_file TEXT NOT NULL,
          file_offset INTEGER NOT NULL,
          error_type TEXT NOT NULL,
          error_message TEXT,
          traceback TEXT,
          created_at INTEGER NOT NULL,
          UNIQUE(source_file, file_offset)
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


def write_email(conn: Connection, email_obj: Email) -> int:
    """
    Upsert an Email record into the 'emails' table using ON CONFLICT(uuid) DO UPDATE.
    Returns the email's integer id.
    """
    sql = """
        INSERT INTO emails (
            uuid, message_id, date, subject, from_addr,
            to_addrs, cc_addrs, bcc_addrs, in_reply_to,
            schema_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(uuid) DO UPDATE SET
            message_id=excluded.message_id,
            date=excluded.date,
            subject=excluded.subject,
            from_addr=excluded.from_addr,
            to_addrs=excluded.to_addrs,
            cc_addrs=excluded.cc_addrs,
            bcc_addrs=excluded.bcc_addrs,
            in_reply_to=excluded.in_reply_to,
            schema_version=excluded.schema_version
        RETURNING id
    """
    date_val = 0
    if email_obj.date is not None:
        date_val = int(email_obj.date.timestamp())

    # Convert lists to comma-separated strings
    to_text = ",".join(email_obj.to_addrs or [])
    cc_text = ",".join(email_obj.cc_addrs or [])
    bcc_text = ",".join(email_obj.bcc_addrs or [])

    cur = conn.execute(
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
    row = cur.fetchone()
    conn.commit()
    if row:
        return int(row[0])
    raise RuntimeError("Failed to retrieve email id after upsert")


def write_dead_letter(
    conn: Connection, source_file: str, offset: int, ex: Exception
) -> int:
    """
    Upsert a dead letter entry to 'dead_letter_queue' on (source_file, file_offset).
    Returns the dead letter's integer id.
    """
    sql2 = """
        INSERT INTO dead_letter_queue (source_file, file_offset, error_type, error_message, traceback, created_at)
        VALUES (?, ?, ?, ?, ?, strftime('%s','now'))
        ON CONFLICT(source_file, file_offset) DO UPDATE SET
            error_type=excluded.error_type,
            error_message=excluded.error_message,
            traceback=excluded.traceback
        RETURNING id
    """
    trace_str = "".join(traceback.format_exception(type(ex), ex, ex.__traceback__))
    cur = conn.execute(
        sql2, (source_file, offset, type(ex).__name__, str(ex), trace_str)
    )
    row = cur.fetchone()
    conn.commit()
    if row:
        return int(row[0])
    raise RuntimeError("Failed to retrieve dead letter id after upsert")


def write_attachment(
    conn: Connection,
    email_id: int,
    storage_service: str,
    storage_address: str,
    attachment_number: int,
    attachment_name: str,
    mime_type: str,
    size: int,
) -> int:
    """
    Upsert an attachment record linked to email_id, unique on (email_id, attachment_number).
    Updates all fields except created_at on conflict.
    Returns the attachment's integer id.
    """
    sql = """
        INSERT INTO attachments (
            email_id, storage_service, storage_address, attachment_number, attachment_name, mime_type, size, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
        ON CONFLICT(email_id, attachment_number) DO UPDATE SET
            storage_service=excluded.storage_service,
            storage_address=excluded.storage_address,
            attachment_name=excluded.attachment_name,
            mime_type=excluded.mime_type,
            size=excluded.size
        RETURNING id
    """
    cur = conn.execute(
        sql,
        (
            email_id,
            storage_service,
            storage_address,
            attachment_number,
            attachment_name,
            mime_type,
            size,
        ),
    )
    row = cur.fetchone()
    conn.commit()
    if row:
        return int(row[0])
    raise RuntimeError("Failed to retrieve attachment id after upsert")
