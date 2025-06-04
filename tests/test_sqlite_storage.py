import sqlite3

import pytest

from postmark.storage.sqlite import create_connection, upgrade


def test_schema_created(tmp_path) -> None:
    """Ensure 'emails' table and schema_version=1 are created."""
    db_path = tmp_path / "test.db"
    conn = create_connection(str(db_path))
    upgrade(conn)

    # Check that 'emails' table exists
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='emails';"
    )
    assert cursor.fetchone() is not None, "'emails' table not created"

    # Check schema_version=1
    cursor = conn.execute("SELECT version FROM schema_version;")
    row = cursor.fetchone()
    assert row is not None and row[0] == 1, "Expected schema_version=1"
    conn.close()
