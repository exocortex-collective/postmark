#!/usr/bin/env python3
"""Backfill emails from Gmail for a given time period.

Usage:
    python scripts/backfill.py [days] [--dry-run]

Examples:
    python scripts/backfill.py 7          # Backfill last 7 days
    python scripts/backfill.py 7 --dry-run # Just count, don't store
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Any

import duckdb
import psycopg2
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Configuration
GMAIL_TOKEN_PATH = os.environ.get("GMAIL_TOKEN_PATH", "/tmp/gmail-token.json")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "10.5.2.21")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "cortex")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cortex")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "cortex")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/tmp/cortex-data/emails.duckdb")


def get_gmail_client() -> Any:
    """Initialize Gmail API client."""
    with open(GMAIL_TOKEN_PATH) as f:
        token_data = json.load(f)

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"],
    )
    return build("gmail", "v1", credentials=creds)


def list_messages(gmail: Any, query: str, max_results: int = 1000) -> list[str]:
    """List message IDs matching the query."""
    message_ids: list[str] = []
    page_token = None

    while len(message_ids) < max_results:
        result = (
            gmail.users()
            .messages()
            .list(
                userId="me",
                q=query,
                maxResults=min(100, max_results - len(message_ids)),
                pageToken=page_token,
            )
            .execute()
        )

        messages = result.get("messages", [])
        message_ids.extend(msg["id"] for msg in messages)

        page_token = result.get("nextPageToken")
        if not page_token:
            break

    return message_ids


def extract_attachment_ids(payload: dict) -> list[str]:
    """Recursively extract attachment IDs from MIME parts."""
    attachment_ids = []

    def recurse(part: dict) -> None:
        body = part.get("body", {})
        if att_id := body.get("attachmentId"):
            attachment_ids.append(att_id)
        for subpart in part.get("parts", []):
            recurse(subpart)

    recurse(payload)
    return attachment_ids


def strip_body(msg: dict) -> dict:
    """Return message dict with body data stripped (for raw_response storage)."""

    def strip_part(part: dict) -> dict:
        result: dict = {}
        for key, value in part.items():
            if key == "body":
                result["body"] = {k: v for k, v in value.items() if k != "data"}
            elif key == "parts":
                result["parts"] = [strip_part(p) for p in value]
            else:
                result[key] = value
        return result

    result: dict = {}
    for key, value in msg.items():
        if key == "payload":
            result["payload"] = strip_part(value)
        else:
            result[key] = value
    return result


def store_message(gmail: Any, gmail_id: str, pg_conn: Any, duck_conn: Any) -> bool:
    """Fetch and store a single message. Returns True if stored, False if skipped."""
    # Check if already exists
    with pg_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM emails_raw WHERE gmail_id = %s", (gmail_id,))
        if cur.fetchone():
            return False

    # Fetch full message
    msg = (
        gmail.users().messages().get(userId="me", id=gmail_id, format="full").execute()
    )

    payload = msg.get("payload", {})
    attachment_ids = extract_attachment_ids(payload)

    # Store in Postgres
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO emails_raw (
                gmail_id, thread_id, history_id, label_ids,
                internal_date, size_estimate, snippet, headers,
                mime_type, attachment_ids, raw_response
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (gmail_id) DO NOTHING
            """,
            (
                gmail_id,
                msg["threadId"],
                msg.get("historyId", ""),
                msg.get("labelIds", []),
                int(msg["internalDate"]),
                msg.get("sizeEstimate"),
                msg.get("snippet", ""),
                json.dumps(payload.get("headers", [])),
                payload.get("mimeType"),
                attachment_ids,
                json.dumps(strip_body(msg)),
            ),
        )

    # Store body in DuckDB
    body_data = payload.get("body", {}).get("data")
    parts = payload.get("parts")
    duck_conn.execute(
        """
        INSERT OR REPLACE INTO email_bodies (gmail_id, parts, body_data)
        VALUES (?, ?, ?)
        """,
        (gmail_id, json.dumps(parts) if parts else None, body_data),
    )

    # Enqueue for parsing
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO queue (queue_name, payload)
            VALUES ('parse', %s)
            """,
            (json.dumps({"gmail_id": gmail_id}),),
        )

    return True


def main() -> None:
    days = 7
    dry_run = False

    for arg in sys.argv[1:]:
        if arg == "--dry-run":
            dry_run = True
        else:
            days = int(arg)

    # Build query for last N days
    after_date = (datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")
    query = f"after:{after_date}"

    print(f"Backfilling messages from last {days} days (after {after_date})...")

    gmail = get_gmail_client()
    message_ids = list_messages(gmail, query, max_results=2000)

    print(f"Found {len(message_ids)} messages")

    if dry_run:
        print("Dry run - not storing messages")
        return

    # Connect to databases
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    pg_conn.autocommit = True

    duck_conn = duckdb.connect(DUCKDB_PATH)
    duck_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS email_bodies (
            gmail_id TEXT PRIMARY KEY,
            parts JSON,
            body_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    stored = 0
    skipped = 0

    for i, gmail_id in enumerate(message_ids):
        try:
            if store_message(gmail, gmail_id, pg_conn, duck_conn):
                stored += 1
                print(f"[{i+1}/{len(message_ids)}] Stored {gmail_id}")
            else:
                skipped += 1
                if skipped <= 5:
                    print(f"[{i+1}/{len(message_ids)}] Skipped {gmail_id} (exists)")
                elif skipped == 6:
                    print("... (suppressing further skip messages)")
        except Exception as e:
            print(f"[{i+1}/{len(message_ids)}] ERROR {gmail_id}: {e}")

    print(f"\nDone! Stored {stored}, skipped {skipped}")
    pg_conn.close()
    duck_conn.close()


if __name__ == "__main__":
    main()
