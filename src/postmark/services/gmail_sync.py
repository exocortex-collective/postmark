"""Gmail sync service - pulls from Pub/Sub, stores raw messages."""

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import schedule
from cortex_utils.metrics import (
    EMAILS_SYNCED,
    ERRORS,
    QUEUE_PROCESSED,
    start_metrics_server,
)
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from prometheus_client import Counter
from psycopg2.extras import Json, execute_values
from psycopg2.pool import ThreadedConnectionPool

from postmark.reliability import (
    BackoffConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    GracefulShutdown,
    call_with_circuit_breaker,
    retry_with_backoff,
)

# Service-specific metrics
PUBSUB_MESSAGES = Counter(
    "gmail_sync_pubsub_messages_total",
    "Total Pub/Sub messages processed",
    ["status"],  # success, error
)

# Queue priority for backfill jobs (lower = deprioritized vs real-time mail)
BACKFILL_PRIORITY = -100

# How often to update backfill progress in database (every N messages)
BACKFILL_PROGRESS_INTERVAL = 50


class BackfillCancelledError(Exception):
    """Raised when a backfill job is cancelled mid-run."""

    pass


def is_retryable_http_error(error: HttpError) -> bool:
    """Check if an HttpError from Gmail API should be retried.

    Retryable: 5XX server errors, 429 rate limit, 503 service unavailable
    Non-retryable: 404 not found, 400 bad request, 403 forbidden
    """
    status = error.resp.status

    # 5XX errors (server-side) and 429 (rate limiting) are retryable.
    if status >= 500 or status == 429:
        return True

    # Other 4XX errors (client-side) are not retryable.
    if 400 <= status < 500:
        return False

    # For any other status, be conservative and retry.
    return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class PostgresConfig:
    """Postgres connection configuration."""

    host: str
    dbname: str
    user: str
    password: str
    port: int = 5432
    pool_min_conn: int = 1
    pool_max_conn: int = 5


@dataclass
class GmailSyncConfig:
    """Configuration for GmailSync service."""

    project_id: str
    subscription_path: str
    topic_name: str
    gmail_token_path: str
    duckdb_api_url: str
    postgres: PostgresConfig

    @classmethod
    def from_env(cls) -> "GmailSyncConfig":
        """Load configuration from environment variables."""
        return cls(
            project_id=os.environ["GCP_PROJECT_ID"],
            subscription_path=os.environ["PUBSUB_SUBSCRIPTION"],
            topic_name=os.environ["PUBSUB_TOPIC"],
            gmail_token_path=os.environ["GMAIL_TOKEN_PATH"],
            duckdb_api_url=os.environ["DUCKDB_API_URL"],
            postgres=PostgresConfig(
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ.get("POSTGRES_PORT", "5432")),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
                pool_min_conn=int(os.environ.get("POSTGRES_POOL_MIN", "1")),
                pool_max_conn=int(os.environ.get("POSTGRES_POOL_MAX", "5")),
            ),
        )


class GmailSync:
    """Main sync service."""

    def __init__(self, config: GmailSyncConfig) -> None:
        self.config = config

        # Thread-safe label cache with lock
        self._label_cache: dict[str, str] = {}
        self._label_cache_lock = threading.Lock()
        self._label_sync_lock = threading.Lock()  # Prevents thundering herd on sync

        # Cache for authenticated user's email address (with lock for thread-safety)
        self._current_user_email: str | None = None
        self._current_user_email_lock = threading.Lock()

        # Thread-local storage for Gmail clients (httplib2 is not thread-safe)
        self._thread_local = threading.local()

        # Reliability: Circuit breaker for DuckDB API
        self._duckdb_circuit = CircuitBreaker(
            "duckdb-api",
            CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=30.0,
                half_open_max_calls=1,
            ),
        )

        # Reliability: Backoff config for Pub/Sub retries
        self._backoff_config = BackoffConfig(
            initial_delay=1.0,
            max_delay=30.0,
            multiplier=2.0,
            jitter=0.1,
            max_retries=3,
        )

        # Reliability: Graceful shutdown handler
        self._shutdown = GracefulShutdown()

        # Initialize clients
        self._init_gmail_token_data()
        self._init_pubsub()
        self._init_postgres()

    def _init_gmail_token_data(self) -> None:
        """Load Gmail token data from file (credentials created per-thread for safety)."""
        with open(self.config.gmail_token_path) as f:
            self._gmail_token_data = json.load(f)
        logger.info("Gmail token data loaded")

    @property
    def gmail(self) -> Resource:
        """Get thread-local Gmail API client.

        httplib2 (used by googleapiclient) is not thread-safe - concurrent
        requests on the same client corrupt SSL state. Each thread gets its
        own client instance via thread-local storage.

        The google.oauth2.credentials.Credentials object is also not thread-safe,
        so a new one is created for each thread.
        """
        if not hasattr(self._thread_local, "gmail"):
            creds = Credentials(**self._gmail_token_data)
            self._thread_local.gmail = build(
                "gmail", "v1", credentials=creds, cache_discovery=False
            )
        return self._thread_local.gmail

    def _init_pubsub(self) -> None:
        """Initialize Pub/Sub subscriber."""
        self.subscriber = pubsub_v1.SubscriberClient()
        logger.info(
            f"Pub/Sub subscriber initialized for {self.config.subscription_path}"
        )

    def _init_postgres(self) -> None:
        """Initialize Postgres connection pool and ensure schema exists."""
        pg = self.config.postgres
        self._pool = ThreadedConnectionPool(
            minconn=pg.pool_min_conn,
            maxconn=pg.pool_max_conn,
            host=pg.host,
            port=pg.port,
            dbname=pg.dbname,
            user=pg.user,
            password=pg.password,
            # TCP keepalive settings to prevent stale connections
            keepalives=1,  # Enable TCP keepalives
            keepalives_idle=30,  # Seconds before starting keepalives
            keepalives_interval=10,  # Seconds between keepalive probes
            keepalives_count=5,  # Number of lost keepalives before disconnect
        )
        # Ensure schema exists using a connection from the pool
        with self._get_conn() as conn:
            self._ensure_postgres_schema(conn)
            conn.commit()
        logger.info("Postgres connection pool initialized")

    @contextmanager
    def _get_conn(self) -> Iterator[psycopg2.extensions.connection]:
        """Get a connection from the pool. Auto-returns on exit.

        Validates the connection before use to detect stale/closed connections.
        If validation fails, discards the connection and retries up to 3 times.
        """
        max_retries = 3
        for attempt in range(max_retries):
            conn = self._pool.getconn()
            conn.autocommit = False
            yielded = False
            try:
                # Validate connection is alive before use
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.rollback()  # Clean up validation transaction
                # Connection is good, yield it
                try:
                    yield conn
                    yielded = True  # Mark as successfully yielded
                finally:
                    # Auto-rollback any uncommitted transaction before returning to pool
                    # This prevents connection leaks when callers forget to commit/rollback
                    if yielded:
                        try:
                            conn.rollback()
                        except Exception:
                            pass  # Connection may be closed or in error state
                return  # Success, exit the context manager
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # Connection is stale/broken
                logger.warning(
                    f"Stale connection detected (attempt {attempt + 1}/{max_retries}): {e}"
                )
                try:
                    conn.close()
                except Exception:
                    pass  # Already broken, ignore close errors
                self._pool.putconn(conn, close=True)  # Remove from pool

                if attempt == max_retries - 1:
                    # Last attempt failed, re-raise the error
                    raise RuntimeError(
                        f"Failed to get valid connection after {max_retries} attempts. "
                        "Check database connectivity and connection pool settings."
                    ) from e
                # Otherwise continue to next retry
            finally:
                # Only return to pool if not successfully yielded
                if not yielded:
                    try:
                        self._pool.putconn(conn)
                    except Exception:
                        pass  # Connection may have been closed already

    def _ensure_postgres_schema(self, conn: psycopg2.extensions.connection) -> None:
        """Create tables if they don't exist."""
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS emails_raw (
                    id BIGSERIAL PRIMARY KEY,
                    gmail_id TEXT UNIQUE NOT NULL,
                    thread_id TEXT NOT NULL,
                    history_id TEXT NOT NULL,
                    label_ids TEXT[],
                    internal_date BIGINT NOT NULL,
                    size_estimate INT,
                    snippet TEXT,
                    headers JSONB NOT NULL,
                    mime_type TEXT,
                    attachment_ids TEXT[],
                    raw_response JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    deleted_at TIMESTAMPTZ  -- Soft delete: set when message deleted from Gmail
                );

                -- Add deleted_at column if it doesn't exist (migration for existing tables)
                ALTER TABLE emails_raw ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

                CREATE INDEX IF NOT EXISTS idx_emails_raw_gmail_id
                    ON emails_raw(gmail_id);
                CREATE INDEX IF NOT EXISTS idx_emails_raw_thread
                    ON emails_raw(thread_id);
                CREATE INDEX IF NOT EXISTS idx_emails_raw_date
                    ON emails_raw(internal_date DESC);

                CREATE TABLE IF NOT EXISTS sync_state (
                    id BIGSERIAL PRIMARY KEY,
                    email_address TEXT UNIQUE NOT NULL,
                    last_history_id TEXT NOT NULL,
                    watch_expiration TIMESTAMPTZ,
                    last_sync_at TIMESTAMPTZ DEFAULT NOW(),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS queue (
                    id BIGSERIAL PRIMARY KEY,
                    queue_name TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    priority INT NOT NULL DEFAULT 0,
                    attempts INT DEFAULT 0,
                    max_attempts INT DEFAULT 3,
                    last_error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    CONSTRAINT valid_status CHECK (
                        status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')
                    )
                );

                -- Drop old index superseded by priority-aware index
                DROP INDEX IF EXISTS idx_queue_pending;

                CREATE INDEX IF NOT EXISTS idx_queue_pending_priority
                    ON queue(queue_name, status, priority DESC, created_at)
                    WHERE status = 'pending';

                -- Index for cancelling pending jobs by gmail_id (used in soft delete)
                CREATE INDEX IF NOT EXISTS idx_queue_pending_gmail_id
                    ON queue((payload->>'gmail_id'))
                    WHERE status = 'pending';

                -- Unique partial indexes for idempotent job processing
                -- Prevents duplicate jobs in pending/processing state
                -- This avoids duplicate work when visibility timeout expires
                -- Note: Must include created_at for partitioned queue table

                -- For parse/triage: dedup by gmail_id
                CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_dedup_gmail
                    ON queue(queue_name, (payload->>'gmail_id'), created_at)
                    WHERE queue_name IN ('parse', 'triage', 'actions')
                      AND status IN ('pending', 'processing');

                -- For attachment: dedup by gmail_id + attachment_id
                CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_dedup_attachment
                    ON queue(queue_name, (payload->>'gmail_id'), (payload->>'attachment_id'), created_at)
                    WHERE queue_name = 'attachment'
                      AND status IN ('pending', 'processing');

                -- Gmail labels cache (id -> name mapping)
                -- name_parts stores hierarchical names as arrays for easy querying
                -- e.g. "Cortex/Todo" -> ['Cortex', 'Todo']
                CREATE TABLE IF NOT EXISTS gmail_labels (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,           -- Original name (e.g. "Cortex/Todo")
                    name_parts TEXT[] NOT NULL,   -- Split hierarchy (e.g. ['Cortex', 'Todo'])
                    type TEXT,  -- 'system' or 'user'
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_gmail_labels_name
                    ON gmail_labels(name);

                -- GIN index for efficient array queries (e.g. WHERE 'Cortex' = ANY(name_parts))
                CREATE INDEX IF NOT EXISTS idx_gmail_labels_parts
                    ON gmail_labels USING GIN(name_parts);

                -- Backfill jobs table for queue-based historical sync
                -- Gateway/CLI insert jobs, gmail-sync polls and executes them
                CREATE TABLE IF NOT EXISTS backfill_jobs (
                    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
                    status TEXT NOT NULL DEFAULT 'pending',
                    query TEXT NOT NULL,
                    days INTEGER,
                    after_date DATE,
                    processed INTEGER DEFAULT 0,
                    stored INTEGER DEFAULT 0,
                    updated INTEGER DEFAULT 0,
                    error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    CONSTRAINT backfill_jobs_valid_status CHECK (
                        status IN ('pending', 'running', 'completed', 'cancelled', 'failed')
                    )
                );

                CREATE INDEX IF NOT EXISTS idx_backfill_jobs_pending
                    ON backfill_jobs(status, created_at)
                    WHERE status IN ('pending', 'running');
            """
            )
        logger.info("Postgres schema ensured")

    def _store_body_via_api(
        self, gmail_id: str, parts: list[Any] | None, body_data: str | None
    ) -> None:
        """Store email body via DuckDB API with circuit breaker protection.

        Raises on failure to allow caller to nack Pub/Sub message for retry.
        Circuit breaker prevents cascade failures when DuckDB API is down.
        """

        def do_request() -> None:
            data = json.dumps(
                {"gmail_id": gmail_id, "parts": parts, "body_data": body_data}
            ).encode()

            req = urllib.request.Request(
                f"{self.config.duckdb_api_url}/body",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    if resp.status != 201:
                        error_body = resp.read().decode(errors="ignore")
                        logger.error(
                            f"DuckDB API returned {resp.status} for {gmail_id}: {error_body}"
                        )
                        raise RuntimeError(
                            f"DuckDB API returned {resp.status} for {gmail_id}"
                        )
            except (urllib.error.URLError, TimeoutError) as e:
                logger.error(f"Failed to store body for {gmail_id}: {e}")
                raise

        try:
            call_with_circuit_breaker(self._duckdb_circuit, do_request)
        except CircuitOpenError:
            logger.warning(
                f"DuckDB API circuit open, skipping body storage for {gmail_id}"
            )
            raise

    def _get_last_history_id(self, email_address: str) -> str | None:
        """Get last synced history ID for an email address."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT last_history_id FROM sync_state WHERE email_address = %s",
                (email_address,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _set_last_history_id(self, email_address: str, history_id: str) -> None:
        """Update last synced history ID."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_state (email_address, last_history_id, last_sync_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (email_address)
                DO UPDATE SET last_history_id = %s, last_sync_at = NOW()
                """,
                (email_address, history_id, history_id),
            )
            conn.commit()

    def _email_exists(self, gmail_id: str) -> bool:
        """Check if email already exists in database."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM emails_raw WHERE gmail_id = %s",
                (gmail_id,),
            )
            return cur.fetchone() is not None

    def _extract_attachment_ids(self, payload: dict[str, Any]) -> list[str]:
        """Recursively extract attachment IDs from MIME parts."""
        attachment_ids = []

        def recurse(part: dict[str, Any]) -> None:
            body = part.get("body", {})
            if att_id := body.get("attachmentId"):
                attachment_ids.append(att_id)
            for subpart in part.get("parts", []):
                recurse(subpart)

        recurse(payload)
        return attachment_ids

    def _strip_body(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Return message dict with body data stripped (for raw_response storage).

        Uses targeted copying to avoid deep copying the entire message.
        Only the payload structure is copied; body.data fields are excluded.
        """

        def strip_part(part: dict[str, Any]) -> dict[str, Any]:
            """Recursively copy a MIME part, excluding body.data."""
            result: dict[str, Any] = {}
            for key, value in part.items():
                if key == "body":
                    # Copy body without the data field
                    result["body"] = {k: v for k, v in value.items() if k != "data"}
                elif key == "parts":
                    # Recursively strip nested parts
                    result["parts"] = [strip_part(p) for p in value]
                else:
                    result[key] = value
            return result

        result: dict[str, Any] = {}
        for key, value in msg.items():
            if key == "payload":
                result["payload"] = strip_part(value)
            else:
                result[key] = value
        return result

    def _update_message_labels(self, gmail_id: str, label_ids: list[str]) -> None:
        """Update label_ids for an existing email in the database."""
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE emails_raw SET label_ids = %s WHERE gmail_id = %s",
                        (label_ids, gmail_id),
                    )
                conn.commit()
                logger.debug(f"Message {gmail_id} exists, updated labels")
            except psycopg2.Error as e:
                conn.rollback()
                logger.error(
                    f"Failed to update labels for {gmail_id}, rolled back: {e}"
                )
                raise

    def store_raw_message(
        self, gmail_id: str, already_exists: bool | None = None, priority: int = 0
    ) -> None:
        """Fetch and store raw message - no parsing, just slam it in.

        Network calls (Gmail API, DuckDB API) happen before the transaction.
        All DB operations for a single message are in one transaction.
        Either everything succeeds or everything rolls back.

        If message already exists, we still update label_ids from Gmail
        (to capture any label changes) but skip body/attachments/parse enqueue.

        If message was deleted (404), logs and returns without error.

        Args:
            gmail_id: The Gmail message ID.
            already_exists: If provided, skips the internal existence check.
            priority: Queue priority for downstream jobs (default 0, negative for backfill).
        """
        if already_exists is None:
            already_exists = self._email_exists(gmail_id)

        # Fetch from Gmail API (outside transaction) - always fetch to get current labels
        try:
            msg = (
                self.gmail.users()
                .messages()
                .get(userId="me", id=gmail_id, format="full")
                .execute()
            )
        except HttpError as e:
            if e.resp.status == 404:
                logger.info(f"Message {gmail_id} not found (deleted), skipping")
                return
            raise

        # If already exists, just update labels and return
        if already_exists:
            self._update_message_labels(gmail_id, msg.get("labelIds", []))
            return

        payload = msg.get("payload", {})
        attachment_ids = self._extract_attachment_ids(payload)

        # Store body via DuckDB API (outside transaction to avoid holding connections)
        # If this fails, exception propagates and Pub/Sub message will be nack'd
        body_data = payload.get("body", {}).get("data")
        parts = payload.get("parts")
        self._store_body_via_api(gmail_id, parts, body_data)

        # All Postgres operations in one transaction with single cursor
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    # Store in Postgres
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
                            json.dumps(self._strip_body(msg)),
                        ),
                    )

                    # Enqueue for parsing (idempotent - skips if already pending/processing)
                    # Uses INSERT ... WHERE NOT EXISTS for partitioned table compatibility
                    cur.execute(
                        """
                        INSERT INTO queue (queue_name, payload, priority)
                        SELECT 'parse', %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM queue
                            WHERE queue_name = 'parse'
                              AND payload->>'gmail_id' = %s
                              AND status IN ('pending', 'processing')
                        )
                        """,
                        (Json({"gmail_id": gmail_id}), priority, gmail_id),
                    )

                    # Enqueue attachments for separate fetch (idempotent)
                    # Uses INSERT ... WHERE NOT EXISTS for partitioned table compatibility
                    for att_id in attachment_ids:
                        cur.execute(
                            """
                            INSERT INTO queue (queue_name, payload, priority)
                            SELECT 'attachment', %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM queue
                                WHERE queue_name = 'attachment'
                                  AND payload->>'gmail_id' = %s
                                  AND payload->>'attachment_id' = %s
                                  AND status IN ('pending', 'processing')
                            )
                            """,
                            (
                                Json({"gmail_id": gmail_id, "attachment_id": att_id}),
                                priority,
                                gmail_id,
                                att_id,
                            ),
                        )

                # Commit the entire message transaction
                conn.commit()
                logger.info(f"Stored message {gmail_id}")

                # Record metrics for stored email and enqueued jobs
                EMAILS_SYNCED.labels(type="new").inc()
                QUEUE_PROCESSED.labels(queue="parse", status="enqueued").inc()
                if attachment_ids:
                    QUEUE_PROCESSED.labels(queue="attachment", status="enqueued").inc(
                        len(attachment_ids)
                    )

            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"Failed to store message {gmail_id}, rolled back: {e}")
                raise

    def _sync_labels_to_db(self) -> None:
        """Fetch all Gmail labels and sync to database.

        This performs a full sync: adds/updates current labels and removes
        any labels that no longer exist in Gmail. Also clears the in-memory
        cache to ensure fresh data on subsequent lookups.
        """
        response = self.gmail.users().labels().list(userId="me").execute()
        labels = response.get("labels", [])

        # Build label data for bulk insert
        # Split hierarchical names into parts (e.g. "Cortex/Todo" -> ["Cortex", "Todo"])
        label_data = [
            (
                label["id"],
                label["name"],
                label["name"].split("/"),
                label.get("type"),
            )
            for label in labels
        ]

        deleted_count = 0
        with self._get_conn() as conn, conn.cursor() as cur:
            if label_data:
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
                    label_data,
                )

                # Remove stale labels that no longer exist in Gmail
                api_label_ids = tuple(label["id"] for label in labels)
                cur.execute(
                    "DELETE FROM gmail_labels WHERE id NOT IN %s",
                    (api_label_ids,),
                )
                deleted_count = cur.rowcount
            else:
                # No labels from API - clear the table
                cur.execute("DELETE FROM gmail_labels")
                deleted_count = cur.rowcount

            conn.commit()

        # Clear in-memory cache to force reload of fresh data
        with self._label_cache_lock:
            self._label_cache.clear()

        logger.info(
            f"Synced {len(labels)} labels to database, removed {deleted_count} stale"
        )

    def _get_label_name(self, label_id: str) -> str | None:
        """Get label name from ID, using thread-safe database cache."""
        # Check in-memory cache first (thread-safe read)
        with self._label_cache_lock:
            if label_id in self._label_cache:
                return self._label_cache[label_id]

        # Check database, with a retry after syncing if not found
        for attempt in range(2):
            with self._get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT name FROM gmail_labels WHERE id = %s",
                    (label_id,),
                )
                row = cur.fetchone()
                if row:
                    name: str = row[0]
                    with self._label_cache_lock:
                        self._label_cache[label_id] = name
                    return name

            # If not found on first try, sync labels and loop to try again
            if attempt == 0:
                with self._label_sync_lock:
                    # Re-check cache - another thread may have synced while we waited
                    with self._label_cache_lock:
                        if label_id in self._label_cache:
                            return self._label_cache[label_id]
                    self._sync_labels_to_db()

        return None

    def _process_label_event(
        self, gmail_id: str, label_ids: list[str], event_type: str
    ) -> None:
        """Process label add/remove events.

        Updates label_ids in emails_raw and enqueues Cortex/* label events
        to the actions queue for workflow dispatch.

        All operations are in one transaction - commit/rollback together.
        """
        if not label_ids:
            return

        # Collect Cortex/* labels for bulk enqueue to actions service
        # (Do this first, before starting transaction, as it may call _sync_labels_to_db)
        actions_to_enqueue = []
        for label_id in label_ids:
            label_name = self._get_label_name(label_id)
            if label_name is None:
                continue

            # Only enqueue Cortex/* labels for workflow dispatch
            if not label_name.startswith("Cortex/"):
                continue

            logger.info(f"Label {event_type}: {label_name} on message {gmail_id}")
            payload = Json(
                {
                    "gmail_id": gmail_id,
                    "label": label_name,
                    "event_type": event_type,
                }
            )
            actions_to_enqueue.append(("actions", payload))

        with self._get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    # Update label_ids array in emails_raw
                    if event_type == "added":
                        # Add labels to array (avoiding duplicates)
                        # Use COALESCE to handle NULL label_ids
                        cur.execute(
                            """
                            UPDATE emails_raw
                            SET label_ids = (
                                SELECT array_agg(DISTINCT elem)
                                FROM unnest(
                                    COALESCE(label_ids, ARRAY[]::text[]) || %s::text[]
                                ) elem
                            )
                            WHERE gmail_id = %s
                            """,
                            (label_ids, gmail_id),
                        )
                    else:  # removed
                        # Remove labels from array
                        # Use COALESCE to return empty array instead of NULL
                        cur.execute(
                            """
                            UPDATE emails_raw
                            SET label_ids = COALESCE((
                                SELECT array_agg(elem)
                                FROM unnest(label_ids) elem
                                WHERE elem != ALL(%s::text[])
                            ), ARRAY[]::text[])
                            WHERE gmail_id = %s
                            """,
                            (label_ids, gmail_id),
                        )

                    # Bulk insert all actions
                    if actions_to_enqueue:
                        execute_values(
                            cur,
                            "INSERT INTO queue (queue_name, payload) VALUES %s",
                            actions_to_enqueue,
                        )

                # Commit the label event transaction
                conn.commit()

            except psycopg2.Error as e:
                conn.rollback()
                logger.error(
                    f"Failed to process label event for {gmail_id}, rolled back: {e}"
                )
                raise

    def _process_message_deleted(self, gmail_id: str) -> None:
        """Soft delete a message and cancel pending queue jobs.

        Sets deleted_at timestamp on emails_raw and cancels any pending
        queue jobs for this message. Preserves the email data for potential
        future use (e.g., spam signal for classifier training).
        """
        with self._get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    # Soft delete the email
                    cur.execute(
                        "UPDATE emails_raw SET deleted_at = NOW() WHERE gmail_id = %s",
                        (gmail_id,),
                    )
                    rows_updated = cur.rowcount

                    # Cancel any pending queue jobs for this message
                    cur.execute(
                        """
                        UPDATE queue SET status = 'cancelled'
                        WHERE status = 'pending'
                        AND payload->>'gmail_id' = %s
                        """,
                        (gmail_id,),
                    )
                    jobs_cancelled = cur.rowcount

                conn.commit()

                if rows_updated > 0:
                    logger.info(
                        f"Soft deleted message {gmail_id}, "
                        f"cancelled {jobs_cancelled} pending jobs"
                    )
                else:
                    logger.debug(f"Message {gmail_id} not found for soft delete")

            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"Failed to soft delete {gmail_id}: {e}")
                raise

    def process_notification(
        self, message: pubsub_v1.subscriber.message.Message
    ) -> None:
        """Handle a single Pub/Sub notification with retry and backoff.

        On transient failures, retries with exponential backoff before nacking.
        This prevents thundering herd on transient failures.
        """
        # Track in-flight work for graceful shutdown
        with self._shutdown.track_work():
            self._process_notification_with_retry(message)

    def _process_notification_with_retry(
        self, message: pubsub_v1.subscriber.message.Message
    ) -> None:
        """Internal notification processor with retry logic."""

        def process_with_error_classification() -> None:
            """Wrapper that catches HttpError and checks if retryable."""
            try:
                self._process_notification_inner(message)
            except HttpError as e:
                # Check if this HttpError should be retried
                if is_retryable_http_error(e):
                    # Re-raise so retry logic can handle it
                    raise
                else:
                    # Non-retryable error (404, 400, 403, etc.) - log and skip
                    logger.warning(
                        f"Non-retryable Gmail API error {e.resp.status}: {e}"
                    )
                    # Acknowledge message to prevent redelivery
                    message.ack()
                    return

        # Only retry truly transient errors
        retryable_exceptions = (
            HttpError,  # Will be filtered by process_with_error_classification
            psycopg2.Error,
            urllib.error.URLError,
            TimeoutError,
            RuntimeError,
            CircuitOpenError,
        )

        try:
            retry_with_backoff(
                process_with_error_classification,
                config=self._backoff_config,
                retryable_exceptions=retryable_exceptions,
            )
            PUBSUB_MESSAGES.labels(status="success").inc()
        except Exception as e:
            # All retries exhausted
            logger.exception(f"Error processing notification after retries: {e}")
            PUBSUB_MESSAGES.labels(status="error").inc()
            ERRORS.labels(service="gmail-sync", error_type="notification_failed").inc()
            message.nack()

    def _fetch_history_pages(
        self, start_history_id: str
    ) -> Iterator[tuple[dict[str, Any], str | None]]:
        """Fetch all history pages starting from a history ID.

        Yields (history_record, final_history_id) tuples for each record.
        The final_history_id is updated as pages are fetched.

        Raises HttpError on API errors (including 404 for expired history).
        """
        page_token: str | None = None
        page_count = 0

        while True:
            response = (
                self.gmail.users()
                .history()
                .list(
                    userId="me",
                    startHistoryId=start_history_id,
                    historyTypes=[
                        "messageAdded",
                        "messageDeleted",
                        "labelAdded",
                        "labelRemoved",
                    ],
                    pageToken=page_token,
                )
                .execute()
            )

            page_count += 1
            page_history_id = response.get("historyId")

            # Log pagination early so we know if failure happens mid-batch
            if page_count == 2:
                logger.info("History API returning multiple pages, fetching all...")

            for record in response.get("history", []):
                yield record, page_history_id

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        if page_count > 1:
            logger.info(f"History fetch completed: {page_count} pages")

    def _process_history_record(self, record: dict[str, Any]) -> tuple[int, int, int]:
        """Process a single history record.

        Returns (messages_added, messages_deleted, label_events) counts.
        """
        messages_added = 0
        messages_deleted = 0
        label_events = 0

        for msg_added in record.get("messagesAdded", []):
            msg_id = msg_added["message"]["id"]
            self.store_raw_message(msg_id)
            messages_added += 1

        for msg_deleted in record.get("messagesDeleted", []):
            msg_id = msg_deleted["message"]["id"]
            self._process_message_deleted(msg_id)
            messages_deleted += 1

        for label_added in record.get("labelsAdded", []):
            msg_id = label_added["message"]["id"]
            label_ids = label_added.get("labelIds", [])
            self._process_label_event(msg_id, label_ids, "added")
            label_events += 1

        for label_removed in record.get("labelsRemoved", []):
            msg_id = label_removed["message"]["id"]
            label_ids = label_removed.get("labelIds", [])
            self._process_label_event(msg_id, label_ids, "removed")
            label_events += 1

        return messages_added, messages_deleted, label_events

    def _process_notification_inner(
        self, message: pubsub_v1.subscriber.message.Message
    ) -> None:
        """Process a Pub/Sub notification for Gmail changes."""
        data = json.loads(message.data.decode("utf-8"))
        email_address = data["emailAddress"]
        new_history_id = data["historyId"]

        logger.info(f"Notification for {email_address}, historyId={new_history_id}")

        last_history_id = self._get_last_history_id(email_address)

        if last_history_id is None:
            logger.info(f"First notification for {email_address}, recording baseline")
            self._set_last_history_id(email_address, new_history_id)
            message.ack()
            return

        # Process all history records across pages
        # If any operation fails, exception propagates and we nack
        # On retry, idempotency (ON CONFLICT DO NOTHING) prevents duplicates
        messages_added = 0
        messages_deleted = 0
        label_events = 0
        final_history_id: str | None = None

        try:
            for record, history_id in self._fetch_history_pages(last_history_id):
                final_history_id = history_id
                added, deleted, labels = self._process_history_record(record)
                messages_added += added
                messages_deleted += deleted
                label_events += labels
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(
                    f"History {last_history_id} too old, resetting to {new_history_id}"
                )
                self._set_last_history_id(email_address, new_history_id)
                message.ack()
                return
            raise

        # Checkpoint after all records processed successfully
        self._set_last_history_id(email_address, final_history_id or new_history_id)

        if messages_added or messages_deleted or label_events:
            logger.info(
                f"Processed history batch: +{messages_added} msgs, "
                f"-{messages_deleted} deleted, {label_events} label events"
            )

        message.ack()

    def _get_current_user_email(self) -> str:
        """Get the email address of the authenticated user (cached, thread-safe)."""
        email = self._current_user_email
        if email is None:
            with self._current_user_email_lock:
                email = self._current_user_email
                if email is None:
                    profile = self.gmail.users().getProfile(userId="me").execute()
                    self._current_user_email = email = str(profile["emailAddress"])
        return email

    def _get_watch_expiration(self) -> datetime | None:
        """Get the current watch expiration time from database."""
        email_address = self._get_current_user_email()
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT watch_expiration FROM sync_state WHERE email_address = %s",
                (email_address,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _check_watch_health(self) -> None:
        """Check watch expiration and log warnings if close to expiring."""
        expiration = self._get_watch_expiration()
        if expiration is None:
            logger.warning("No watch expiration found - watch may not be set up")
            return

        now = datetime.now(expiration.tzinfo)
        time_until_expiry = expiration - now

        if time_until_expiry.total_seconds() < 0:
            logger.critical(
                f"Watch has EXPIRED at {expiration}! "
                "Push notifications are not being received."
            )
        elif time_until_expiry < timedelta(days=1):
            logger.warning(
                f"Watch expiring soon: {expiration} "
                f"({time_until_expiry.total_seconds() / 3600:.1f} hours remaining)"
            )
        else:
            logger.info(
                f"Watch healthy, expires {expiration} "
                f"({time_until_expiry.days} days remaining)"
            )

    def renew_watch(self) -> None:
        """Renew Gmail push notification registration.

        Raises on failure so errors are visible to alerting systems.
        """
        try:
            response = (
                self.gmail.users()
                .watch(
                    userId="me",
                    body={
                        "topicName": self.config.topic_name,
                        "labelIds": ["INBOX"],
                        "labelFilterBehavior": "INCLUDE",
                    },
                )
                .execute()
            )

            expiration_ms = int(response["expiration"])
            expiration_dt = datetime.fromtimestamp(
                expiration_ms / 1000, tz=timezone.utc
            )
            history_id = str(response["historyId"])

            # Get the actual email address for this user
            email_address = self._get_current_user_email()

            with self._get_conn() as conn, conn.cursor() as cur:
                # Check if user already has sync state
                cur.execute(
                    "SELECT id FROM sync_state WHERE email_address = %s",
                    (email_address,),
                )
                if cur.fetchone():
                    # Existing user - just update watch expiration
                    cur.execute(
                        """
                        UPDATE sync_state
                        SET watch_expiration = %s, last_sync_at = NOW()
                        WHERE email_address = %s
                        """,
                        (expiration_dt, email_address),
                    )
                else:
                    # New user - use historyId from watch() as baseline
                    cur.execute(
                        """
                        INSERT INTO sync_state
                            (email_address, last_history_id, watch_expiration)
                        VALUES (%s, %s, %s)
                        """,
                        (email_address, history_id, expiration_dt),
                    )
                    logger.info(f"New sync state created for {email_address}")

                conn.commit()
            logger.info(
                f"Watch renewed for {email_address}, expires at {expiration_dt}"
            )

        except Exception as e:
            # Log at CRITICAL level so alerting systems catch this
            # Use exc_info=True to preserve stack trace for debugging
            logger.critical(f"FAILED to renew Gmail watch: {e}", exc_info=True)
            # Re-raise so the error is visible and can trigger alerts
            raise

    def _update_backfill_progress(
        self, job_id: str, processed: int, stored: int, updated: int
    ) -> None:
        """Update backfill job progress in database."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE backfill_jobs
                SET processed = %s, stored = %s, updated = %s
                WHERE id = %s
                """,
                (processed, stored, updated, job_id),
            )
            conn.commit()

    def _is_backfill_cancelled(self, job_id: str) -> bool:
        """Check if a backfill job has been cancelled."""
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM backfill_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
            return row is not None and row[0] == "cancelled"

    def backfill(
        self,
        days: int | None = None,
        after: str | None = None,
        job_id: str | None = None,
    ) -> tuple[int, int, int]:
        """Backfill emails from a date.

        Fetches message IDs via messages.list, stores any missing messages,
        and updates the history baseline if we found newer messages.

        Args:
            days: Number of days back from today (e.g., 7 for last week)
            after: Absolute date in YYYY-MM-DD format (e.g., "2025-01-01")
            job_id: Optional backfill_jobs.id for progress tracking and cancellation

        Returns:
            Tuple of (processed, stored, updated) - total checked, new, and label updates

        Raises:
            BackfillCancelledError: If job was cancelled mid-run
        """
        if after:
            # Parse absolute date with validation
            try:
                after_date = datetime.strptime(after, "%Y-%m-%d").strftime("%Y/%m/%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format: '{after}'. Expected YYYY-MM-DD (e.g., 2025-01-01)"
                )
            logger.info(f"Backfilling messages after {after_date}")
        elif days:
            after_date = (datetime.now() - timedelta(days=days)).strftime("%Y/%m/%d")
            logger.info(
                f"Backfilling messages from last {days} days (after {after_date})"
            )
        else:
            raise ValueError("Must provide either 'days' or 'after' parameter")

        query = f"after:{after_date}"

        stored = 0
        updated = 0
        processed = 0
        newest_message_id: str | None = None
        page_token: str | None = None

        # Process messages page by page to avoid loading all IDs into memory
        while True:
            # Check for cancellation between pages
            if job_id and self._is_backfill_cancelled(job_id):
                logger.info(f"Backfill job {job_id} cancelled, stopping")
                self._update_backfill_progress(job_id, processed, stored, updated)
                raise BackfillCancelledError(f"Job {job_id} was cancelled")

            result = (
                self.gmail.users()
                .messages()
                .list(
                    userId="me",
                    q=query,
                    maxResults=100,
                    pageToken=page_token,
                )
                .execute()
            )

            messages = result.get("messages", [])
            if not messages:
                break

            # Track newest message ID from first page (newest first)
            if newest_message_id is None and messages:
                newest_message_id = messages[0]["id"]

            # Process this page (reverse to go oldest first within page)
            for msg in reversed(messages):
                gmail_id = msg["id"]
                already_exists = self._email_exists(gmail_id)

                try:
                    self.store_raw_message(
                        gmail_id,
                        already_exists=already_exists,
                        priority=BACKFILL_PRIORITY,
                    )
                    if already_exists:
                        updated += 1
                    else:
                        stored += 1
                    processed += 1

                    # Update progress in database periodically
                    if job_id and processed % BACKFILL_PROGRESS_INTERVAL == 0:
                        self._update_backfill_progress(
                            job_id, processed, stored, updated
                        )
                        logger.info(
                            f"Backfill progress: processed {processed}, "
                            f"stored {stored}, updated {updated}"
                        )

                except (
                    HttpError,
                    psycopg2.Error,
                    urllib.error.URLError,
                    TimeoutError,
                    RuntimeError,
                ) as e:
                    logger.error(f"Failed to process {gmail_id}: {e}")
                    processed += 1

            page_token = result.get("nextPageToken")
            if not page_token:
                break

        logger.info(f"Processed {stored + updated} messages total")

        # Update history baseline if we stored new messages
        if newest_message_id and stored > 0:
            # Get historyId from the newest message
            newest_msg = (
                self.gmail.users()
                .messages()
                .get(userId="me", id=newest_message_id, format="minimal")
                .execute()
            )
            newest_history_id = newest_msg.get("historyId")

            if newest_history_id:
                email_address = self._get_current_user_email()
                current_history_id = self._get_last_history_id(email_address)

                # Only update if we don't have a baseline yet, or if this is newer
                # historyIds are numeric strings, higher = newer
                if current_history_id is None or (
                    newest_history_id
                    and int(newest_history_id) > int(current_history_id)
                ):
                    self._set_last_history_id(email_address, newest_history_id)
                    logger.info(f"Updated history baseline to {newest_history_id}")

        logger.info(
            f"Backfill complete: processed {processed}, stored {stored}, updated {updated}"
        )
        return processed, stored, updated

    def check_backfill_queue(self) -> bool:
        """Check for and process one pending backfill job.

        Returns True if a job was found and processed (or failed/cancelled).
        Returns False if no pending jobs.

        This method is designed to be called periodically from the main loop,
        processing one job at a time to avoid blocking real-time mail.
        """
        # Claim a pending job atomically
        with self._get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE backfill_jobs
                SET status = 'running', started_at = NOW()
                WHERE id = (
                    SELECT id FROM backfill_jobs
                    WHERE status = 'pending'
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, query, days, after_date
                """
            )
            row = cur.fetchone()
            conn.commit()

            if not row:
                return False

            job_id, query, days, after_date = row

        logger.info(f"Starting backfill job {job_id}: {query}")

        try:
            # Run backfill with job tracking
            if after_date:
                processed, stored, updated = self.backfill(
                    after=after_date.strftime("%Y-%m-%d"), job_id=job_id
                )
            elif days:
                processed, stored, updated = self.backfill(days=days, job_id=job_id)
            else:
                # Query was provided directly (shouldn't happen in normal flow)
                raise ValueError(f"Invalid job {job_id}: no days or after_date")

            # Mark completed
            with self._get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE backfill_jobs
                    SET status = 'completed', completed_at = NOW(),
                        processed = %s, stored = %s, updated = %s
                    WHERE id = %s
                    """,
                    (processed, stored, updated, job_id),
                )
                conn.commit()

            logger.info(
                f"Backfill job {job_id} completed: {stored} stored, {updated} updated"
            )

        except BackfillCancelledError:
            # Job was cancelled - already logged, just update final counts
            with self._get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE backfill_jobs SET completed_at = NOW() WHERE id = %s",
                    (job_id,),
                )
                conn.commit()
            logger.info(f"Backfill job {job_id} cancelled")

        except Exception as e:
            # Mark failed with error message
            with self._get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE backfill_jobs
                    SET status = 'failed', completed_at = NOW(), error = %s
                    WHERE id = %s
                    """,
                    (str(e)[:1000], job_id),  # Truncate error to avoid huge strings
                )
                conn.commit()
            logger.error(f"Backfill job {job_id} failed: {e}")

        return True

    def fallback_poll(self) -> None:
        """Hourly fallback poll to catch missed notifications."""
        logger.info("Running fallback poll")
        # This would fetch recent messages and compare with what we have
        # For now, just log - can implement if needed
        pass

    def run_scheduler(self) -> None:
        """Run the scheduler loop."""
        while True:
            schedule.run_pending()
            time.sleep(60)

    def _safe_renew_watch(self) -> None:
        """Wrapper for scheduled watch renewal that won't crash scheduler."""
        try:
            self.renew_watch()
        except Exception as e:
            # Already logged at CRITICAL level in renew_watch()
            # Don't re-raise here to keep scheduler running
            logger.debug(f"Handled exception during scheduled watch renewal: {e!r}")

    def _safe_check_backfill_queue(self) -> None:
        """Wrapper for backfill queue check that won't crash scheduler."""
        try:
            self.check_backfill_queue()
        except Exception as e:
            logger.error(f"Error checking backfill queue: {e}", exc_info=True)
            ERRORS.labels(
                service="gmail-sync", error_type="backfill_check_failed"
            ).inc()

    def run(self) -> None:
        """Main entry point - start the service."""
        logger.info("Starting Gmail sync service")

        # Check watch health and renew on startup
        self._check_watch_health()
        self.renew_watch()

        # Schedule recurring tasks
        # Renew watch every 12 hours (watch expires after 7 days, so plenty of buffer)
        schedule.every(12).hours.do(self._safe_renew_watch)
        schedule.every().hour.do(self.fallback_poll)
        # Check for queued backfill jobs every minute
        schedule.every(1).minutes.do(self._safe_check_backfill_queue)

        # Start scheduler in background thread
        scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()

        # Start Pub/Sub subscriber with flow control to prevent connection pool exhaustion
        # Limit concurrent messages to match pool size
        flow_control = FlowControl(max_messages=self.config.postgres.pool_max_conn)
        streaming_pull_future = self.subscriber.subscribe(
            self.config.subscription_path,
            callback=self.process_notification,
            flow_control=flow_control,
        )
        logger.info(f"Listening on {self.config.subscription_path}")

        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            logger.info("Shutdown signal received, initiating graceful shutdown...")
            self._shutdown.initiate()
            streaming_pull_future.cancel()

            # Wait for in-flight notifications to complete
            if self._shutdown.wait_for_completion(timeout=30.0):
                logger.info("Graceful shutdown complete - all work finished")
            else:
                logger.warning(
                    "Graceful shutdown timeout - some work may be incomplete"
                )

            streaming_pull_future.result()
            logger.info("Shutdown complete")


def main() -> None:
    """Entry point.

    Usage:
        python -m postmark.services.gmail_sync                    # Normal sync mode
        python -m postmark.services.gmail_sync --backfill 7       # Backfill 7 days then sync
        python -m postmark.services.gmail_sync --backfill-only 7  # Backfill only, don't sync
        python -m postmark.services.gmail_sync --backfill-from 2025-01-01  # Backfill from date
    """
    import argparse

    parser = argparse.ArgumentParser(description="Gmail sync service")
    parser.add_argument(
        "--backfill",
        type=int,
        metavar="DAYS",
        help="Backfill N days of email before starting sync",
    )
    parser.add_argument(
        "--backfill-only",
        type=int,
        metavar="DAYS",
        help="Backfill N days of email and exit (don't start sync)",
    )
    parser.add_argument(
        "--backfill-from",
        type=str,
        metavar="DATE",
        help="Backfill from date (YYYY-MM-DD) and exit",
    )
    args = parser.parse_args()

    # Start metrics server
    metrics_port = int(os.environ.get("METRICS_PORT", "8000"))
    start_metrics_server(port=metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")

    config = GmailSyncConfig.from_env()
    sync = GmailSync(config)

    # Handle backfill modes
    if args.backfill_from:
        processed, stored, updated = sync.backfill(after=args.backfill_from)
        print(
            f"Backfill complete: {processed} processed, {stored} stored, {updated} updated"
        )
        return

    if args.backfill_only:
        processed, stored, updated = sync.backfill(days=args.backfill_only)
        print(
            f"Backfill complete: {processed} processed, {stored} stored, {updated} updated"
        )
        return

    if args.backfill:
        sync.backfill(days=args.backfill)

    sync.run()


if __name__ == "__main__":
    main()
