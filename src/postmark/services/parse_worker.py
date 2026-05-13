#!/usr/bin/env python3
"""Parse worker service - processes raw emails from queue into structured data."""

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.errors
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import (
    EMAILS_PARSED,
    ERRORS,
    PROCESSING_DURATION,
    QUEUE_PROCESSED,
    start_metrics_server,
)
from cortex_utils.queue.retry import fail_or_retry, ready_predicate
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from psycopg2.extras import Json, execute_values

from postmark.email.gmail_parser import ParsedEmail, parse_gmail_message
from postmark.reliability import GracefulShutdown
from postmark.storage.duckdb_client import get_body_data_from_api

logger = get_logger(__name__)

# How long a job can be in 'processing' before it's considered stale
# and reset to 'pending' (visibility timeout)
VISIBILITY_TIMEOUT_MINUTES = 5
MAX_ATTEMPTS = 3


@dataclass
class Config:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    duckdb_api_url: str
    gmail_token_path: str
    batch_size: int = 100
    poll_interval: int = 5  # seconds between queue polls when empty


class ParseWorker:
    def __init__(self, config: Config):
        self.config = config
        self._shutdown = GracefulShutdown()
        self._pg_conn: psycopg2.extensions.connection | None = None
        self._gmail: Any = None
        self._label_cache: set[str] = set()  # Known label IDs in gmail_labels
        self._last_label_sync: float = 0  # Timestamp of last Gmail API label sync
        self._label_sync_interval: float = 60.0  # Min seconds between API syncs

    def _get_pg_conn(self) -> psycopg2.extensions.connection:
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = psycopg2.connect(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                dbname=self.config.postgres_db,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
            )
        return self._pg_conn

    def _get_gmail(self) -> Any:
        """Get Gmail API client, initializing if needed."""
        if self._gmail is None:
            with open(self.config.gmail_token_path, encoding="utf-8") as f:
                token_data = json.load(f)
            creds = Credentials.from_authorized_user_info(token_data)
            self._gmail = build("gmail", "v1", credentials=creds, cache_discovery=False)
            logger.info("Gmail API client initialized")
        return self._gmail

    def _load_known_labels(self) -> None:
        """Load existing label IDs from gmail_labels into cache."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM gmail_labels")
            self._label_cache = {row[0] for row in cur.fetchall()}
        logger.info(f"Loaded {len(self._label_cache)} known labels into cache")

    def _sync_labels(self, label_ids: list[str]) -> None:
        """Ensure all label_ids exist in gmail_labels table.

        Checks cache first, then fetches missing labels from Gmail API.
        Rate-limited to avoid excessive API calls.
        """
        if not label_ids:
            return

        # Find labels not in cache (set difference is more efficient)
        missing = list(set(label_ids) - self._label_cache)
        if not missing:
            return

        # Rate limit: skip API call if we synced recently
        now = time.time()
        if now - self._last_label_sync < self._label_sync_interval:
            logger.debug(
                f"Skipping label sync, {len(missing)} labels missing but "
                f"synced {now - self._last_label_sync:.0f}s ago"
            )
            return

        # Fetch all labels from Gmail API (more efficient than individual lookups)
        gmail = self._get_gmail()
        response = gmail.users().labels().list(userId="me").execute()
        api_labels = response.get("labels", [])

        # Build lookup and data for insert
        api_label_map = {label["id"]: label for label in api_labels}
        labels_to_insert = []
        not_found = []

        for label_id in missing:
            if label_id in api_label_map:
                label = api_label_map[label_id]
                labels_to_insert.append(
                    (
                        label["id"],
                        label["name"],
                        label["name"].split("/"),
                        label.get("type"),
                    )
                )
            else:
                not_found.append(label_id)

        if not_found:
            logger.warning(
                f"Labels not found in Gmail API (possibly deleted): {not_found}"
            )

        if labels_to_insert:
            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO gmail_labels (id, name, name_parts, type)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        name_parts = EXCLUDED.name_parts,
                        type = EXCLUDED.type,
                        updated_at = NOW()
                    """,
                    labels_to_insert,
                )
                conn.commit()

            logger.info(f"Synced {len(labels_to_insert)} new labels to gmail_labels")

        # Update timestamp only after successful DB commit for robustness
        self._last_label_sync = time.time()

        # Update cache with ALL labels from API (not just missing ones)
        # This ensures future lookups benefit from the full sync
        self._label_cache.update(label["id"] for label in api_labels)

    def _ensure_schema(self) -> None:
        """Ensure emails_parsed table exists."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS emails_parsed (
                    id BIGSERIAL PRIMARY KEY,
                    gmail_id TEXT UNIQUE NOT NULL,
                    from_addr TEXT,
                    from_name TEXT,
                    to_addrs TEXT[],
                    cc_addrs TEXT[],
                    bcc_addrs TEXT[],
                    subject TEXT,
                    date_header TIMESTAMPTZ,
                    message_id TEXT,
                    in_reply_to TEXT,
                    refs TEXT[],
                    body_text TEXT,
                    body_html TEXT,
                    parsed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_emails_parsed_gmail_id
                ON emails_parsed(gmail_id)
            """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_emails_parsed_from
                ON emails_parsed(from_addr)
            """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_emails_parsed_date
                ON emails_parsed(date_header DESC)
            """
            )
            conn.commit()
        logger.info("emails_parsed schema ensured")

    def _claim_jobs(self, batch_size: int) -> list[dict[str, Any]]:
        """Claim a batch of pending parse jobs.

        Also resets stale jobs that have been processing for too long
        (visibility timeout), handling crashed/killed workers.
        """
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            # Reset stale jobs and claim new ones in a single transaction
            cur.execute(
                f"""
                WITH reset_stale AS (
                    UPDATE queue
                    SET status = 'pending', claimed_at = NULL
                    WHERE queue_name = 'parse'
                      AND status = 'processing'
                      AND claimed_at < NOW() - (INTERVAL '1 minute' * %s)
                ),
                claimable AS (
                    SELECT id FROM queue
                    WHERE queue_name = 'parse' AND status = 'pending'
                      AND {ready_predicate()}
                    ORDER BY priority DESC, created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE queue
                SET status = 'processing', claimed_at = NOW()
                WHERE id IN (SELECT id FROM claimable)
                RETURNING id, payload, priority
                """,
                (VISIBILITY_TIMEOUT_MINUTES, batch_size),
            )
            jobs = [
                {"id": row[0], "payload": row[1], "priority": row[2]}
                for row in cur.fetchall()
            ]
            conn.commit()
        return jobs

    def _get_raw_email(self, gmail_id: str) -> dict[str, Any] | None:
        """Fetch raw email from Postgres."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT gmail_id, thread_id, headers, internal_date, snippet, mime_type,
                       label_ids
                FROM emails_raw
                WHERE gmail_id = %s
            """,
                (gmail_id,),
            )
            row = cur.fetchone()
            if row:
                return {
                    "gmail_id": row[0],
                    "thread_id": row[1],
                    "headers": row[2],
                    "internal_date": row[3],
                    "snippet": row[4],
                    "mime_type": row[5],
                    "label_ids": row[6] or [],
                }
        return None

    def _store_parsed(self, parsed: ParsedEmail) -> None:
        """Store parsed email in Postgres."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO emails_parsed (
                    gmail_id, from_addr, from_name, to_addrs, cc_addrs, bcc_addrs,
                    subject, date_header, message_id, in_reply_to, refs,
                    body_text, body_html
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (gmail_id) DO UPDATE SET
                    from_addr = EXCLUDED.from_addr,
                    from_name = EXCLUDED.from_name,
                    to_addrs = EXCLUDED.to_addrs,
                    cc_addrs = EXCLUDED.cc_addrs,
                    bcc_addrs = EXCLUDED.bcc_addrs,
                    subject = EXCLUDED.subject,
                    date_header = EXCLUDED.date_header,
                    message_id = EXCLUDED.message_id,
                    in_reply_to = EXCLUDED.in_reply_to,
                    refs = EXCLUDED.refs,
                    body_text = EXCLUDED.body_text,
                    body_html = EXCLUDED.body_html,
                    parsed_at = NOW()
            """,
                (
                    parsed.gmail_id,
                    parsed.from_addr,
                    parsed.from_name,
                    parsed.to_addrs,
                    parsed.cc_addrs,
                    parsed.bcc_addrs,
                    parsed.subject,
                    parsed.date_header,
                    parsed.message_id,
                    parsed.in_reply_to,
                    parsed.references,
                    parsed.body_text,
                    parsed.body_html,
                ),
            )
            conn.commit()

    def _complete_job(self, job_id: int) -> None:
        """Mark job as completed."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE queue
                SET status = 'completed', completed_at = NOW()
                WHERE id = %s
            """,
                (job_id,),
            )
            conn.commit()

    def _fail_job(self, job_id: int, error: str) -> None:
        """Schedule a retry with backoff, or mark failed if attempts exhausted."""
        conn = self._get_pg_conn()
        fail_or_retry(conn, job_id, error, max_attempts=MAX_ATTEMPTS)
        conn.commit()

    def _enqueue_triage(self, gmail_id: str, priority: int = 0) -> None:
        """Enqueue email for triage (idempotent - skips if already pending/processing).

        Uses INSERT ... WHERE NOT EXISTS for partitioned table compatibility.
        Handles race conditions gracefully by catching UniqueViolation.
        """
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO queue (queue_name, payload, priority)
                    SELECT 'triage', %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM queue
                        WHERE queue_name = 'triage'
                          AND payload->>'gmail_id' = %s
                          AND status IN ('pending', 'processing')
                    )
                    """,
                    (Json({"gmail_id": gmail_id}), priority, gmail_id),
                )
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                logger.debug(
                    f"Triage job for {gmail_id} already exists (race condition). Ignoring."
                )

    def process_job(self, job: dict[str, Any]) -> bool:
        """Process a single parse job. Returns True on success."""
        gmail_id = job["payload"]["gmail_id"]
        start_time = time.time()

        try:
            # Get raw email
            raw = self._get_raw_email(gmail_id)
            if not raw:
                logger.warning(f"Raw email not found: {gmail_id}")
                self._fail_job(job["id"], "Raw email not found")
                return False

            # Ensure all labels for this email exist in gmail_labels
            self._sync_labels(raw["label_ids"])

            # Get body data from DuckDB API
            body_data = get_body_data_from_api(self.config.duckdb_api_url, gmail_id)

            # Parse email
            parsed = parse_gmail_message(raw, body_data)

            # Store parsed result
            self._store_parsed(parsed)
            EMAILS_PARSED.inc()

            # Enqueue for triage (classification and routing)
            # Propagate priority so backfill jobs stay deprioritized through pipeline
            self._enqueue_triage(gmail_id, priority=job.get("priority", 0))

            # Mark job complete
            self._complete_job(job["id"])

            # Record processing duration
            PROCESSING_DURATION.labels(queue="parse", operation="process").observe(
                time.time() - start_time
            )

            return True

        except Exception as e:
            logger.error(f"Failed to parse {gmail_id}: {e}")
            self._fail_job(job["id"], str(e))
            return False

    def run(self) -> None:
        """Main processing loop with graceful shutdown support."""
        self._ensure_schema()
        self._load_known_labels()

        logger.info("Parse worker started")

        processed = 0
        failed = 0

        while self._shutdown.should_continue():
            jobs = self._claim_jobs(self.config.batch_size)

            if not jobs:
                logger.debug(f"No jobs, sleeping {self.config.poll_interval}s")
                # Check shutdown more frequently than poll interval
                for _ in range(self.config.poll_interval):
                    if not self._shutdown.should_continue():
                        break
                    time.sleep(1)
                continue

            logger.info(f"Processing batch of {len(jobs)} jobs")

            for job in jobs:
                if not self._shutdown.should_continue():
                    logger.info("Shutdown requested, finishing current batch")
                    break

                with self._shutdown.track_work():
                    if self.process_job(job):
                        processed += 1
                        QUEUE_PROCESSED.labels(queue="parse", status="success").inc()
                    else:
                        failed += 1
                        QUEUE_PROCESSED.labels(queue="parse", status="error").inc()
                        ERRORS.labels(
                            service="parse-worker", error_type="job_failed"
                        ).inc()

                if (processed + failed) % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {failed} failed")

        # Wait for any remaining in-flight work
        self._shutdown.wait_for_completion(timeout=30.0)
        logger.info(f"Parse worker stopped: {processed} processed, {failed} failed")

    def run_once(self) -> tuple[int, int]:
        """Process all pending jobs once, then exit. Returns (processed, failed)."""
        self._ensure_schema()
        self._load_known_labels()

        logger.info("Parse worker running once")

        processed = 0
        failed = 0

        while True:
            jobs = self._claim_jobs(self.config.batch_size)

            if not jobs:
                break

            logger.info(f"Processing batch of {len(jobs)} jobs")

            for job in jobs:
                if self.process_job(job):
                    processed += 1
                    QUEUE_PROCESSED.labels(queue="parse", status="success").inc()
                else:
                    failed += 1
                    QUEUE_PROCESSED.labels(queue="parse", status="error").inc()
                    ERRORS.labels(service="parse-worker", error_type="job_failed").inc()

                if (processed + failed) % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {failed} failed")

        logger.info(f"Parse worker complete: {processed} processed, {failed} failed")
        return processed, failed

    def stop(self) -> None:
        """Signal the worker to stop gracefully."""
        logger.info("Initiating graceful shutdown...")
        self._shutdown.initiate()


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse worker service")
    parser.add_argument(
        "--once", action="store_true", help="Process all pending jobs once and exit"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of jobs to process per batch (default: 100)",
    )
    args = parser.parse_args()

    # Configure logging
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    configure_logging(service_name="parse-worker", level=log_level)

    # All config from environment - use .env.template as reference
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
        logger.error("See .env.template for required configuration")
        sys.exit(1)

    # Start metrics server
    metrics_port = int(os.environ.get("METRICS_PORT", "8001"))
    start_metrics_server(port=metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")

    config = Config(
        postgres_host=os.environ["POSTGRES_HOST"],
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        duckdb_api_url=os.environ["DUCKDB_API_URL"],
        gmail_token_path=os.environ["GMAIL_TOKEN_PATH"],
        batch_size=args.batch_size,
    )

    worker = ParseWorker(config)

    # Handle signals for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        worker.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    if args.once:
        processed, failed = worker.run_once()
        sys.exit(0 if failed == 0 else 1)
    else:
        worker.run()


if __name__ == "__main__":
    main()
