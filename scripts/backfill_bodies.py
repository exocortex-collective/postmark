#!/usr/bin/env python3
"""Backfill DuckDB email bodies from Gmail API.

This script finds emails that exist in Postgres (emails_raw) but not in DuckDB
(email_bodies), and fetches the body data from Gmail API.

Usage:
    # Check what needs backfilling (dry run)
    uv run python scripts/backfill_bodies.py --dry-run

    # Backfill all missing bodies
    uv run python scripts/backfill_bodies.py

    # Backfill with batch size limit
    uv run python scripts/backfill_bodies.py --batch-size 100
"""

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from typing import Any

import psycopg2
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def get_gmail_ids_in_postgres(pg_conn: Any) -> set[str]:
    """Get all gmail_ids from emails_raw table."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT gmail_id FROM emails_raw")
        return {row[0] for row in cur}


def get_gmail_ids_in_duckdb(api_url: str) -> set[str]:
    """Get all gmail_ids from DuckDB via API."""
    try:
        url = f"{api_url}/gmail_ids"
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return set(data.get("gmail_ids", []))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Endpoint might not exist yet, return empty set
            logger.warning("DuckDB /gmail_ids endpoint not found, assuming empty")
            return set()
        raise


def store_body_via_api(
    api_url: str, gmail_id: str, parts: list[Any] | None, body_data: str | None
) -> bool:
    """Store email body via DuckDB API. Returns True on success."""
    data = json.dumps(
        {"gmail_id": gmail_id, "parts": parts, "body_data": body_data}
    ).encode()

    req = urllib.request.Request(
        f"{api_url}/body",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return bool(resp.status == 201)
    except (urllib.error.URLError, TimeoutError) as e:
        logger.error(f"Failed to store body for {gmail_id}: {e}")
        return False


def fetch_and_store_body(gmail_service: Any, api_url: str, gmail_id: str) -> bool:
    """Fetch message from Gmail and store body in DuckDB."""
    try:
        msg = (
            gmail_service.users()
            .messages()
            .get(userId="me", id=gmail_id, format="full")
            .execute()
        )

        payload = msg.get("payload", {})
        body_data = payload.get("body", {}).get("data")
        parts = payload.get("parts")

        return store_body_via_api(api_url, gmail_id, parts, body_data)
    except HttpError as e:
        logger.error(f"Failed to fetch/store {gmail_id}: {e}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill DuckDB email bodies")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show what would be backfilled, don't fetch",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Limit number of messages to process (0 = all)",
    )
    args = parser.parse_args()

    # Load config from environment
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "DUCKDB_API_URL",
        "GMAIL_TOKEN_PATH",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Connect to Postgres
    pg_conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        api_url = os.environ["DUCKDB_API_URL"]

        # Get IDs from both sources
        logger.info("Fetching gmail_ids from Postgres...")
        postgres_ids = get_gmail_ids_in_postgres(pg_conn)
        logger.info(f"Found {len(postgres_ids)} emails in Postgres")

        logger.info("Fetching gmail_ids from DuckDB...")
        duckdb_ids = get_gmail_ids_in_duckdb(api_url)
        logger.info(f"Found {len(duckdb_ids)} bodies in DuckDB")

        # Find missing
        missing_ids = postgres_ids - duckdb_ids
        logger.info(f"Missing {len(missing_ids)} bodies in DuckDB")

        if not missing_ids:
            logger.info("Nothing to backfill!")
            return

        if args.dry_run:
            logger.info("Dry run - would backfill these gmail_ids:")
            for gid in list(missing_ids)[:20]:
                print(f"  {gid}")
            if len(missing_ids) > 20:
                print(f"  ... and {len(missing_ids) - 20} more")
            return

        # Initialize Gmail client
        with open(os.environ["GMAIL_TOKEN_PATH"]) as f:
            token_data = json.load(f)

        creds = Credentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"],
        )
        gmail = build("gmail", "v1", credentials=creds)
        logger.info("Gmail client initialized")

        # Process missing bodies
        to_process = list(missing_ids)
        if args.batch_size > 0:
            to_process = to_process[: args.batch_size]
            logger.info(
                f"Processing batch of {len(to_process)} (limited by --batch-size)"
            )

        success = 0
        failed = 0

        for i, gmail_id in enumerate(to_process):
            if fetch_and_store_body(gmail, api_url, gmail_id):
                success += 1
            else:
                failed += 1

            if (i + 1) % 100 == 0:
                logger.info(
                    f"Progress: {i + 1}/{len(to_process)} "
                    f"({success} success, {failed} failed)"
                )

        logger.info(f"Backfill complete: {success} success, {failed} failed")
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
