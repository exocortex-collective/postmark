"""Gmail sync service - pulls from Pub/Sub, stores raw messages."""

import json
import os
import signal
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import schedule
from cortex_utils.learning import (
    ADD,
    REMOVE,
    active_default_label,
    active_label_prefix,
    ensure_learning_schema,
    expected_cortex_labels,
    record_learning_opportunity,
)
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import (
    EMAILS_SYNCED,
    ERRORS,
    QUEUE_PROCESSED,
    start_metrics_server,
)
from cortex_utils.queue import enqueue, ensure_queue_schema
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from prometheus_client import Counter
from psycopg2 import sql
from psycopg2.extras import Json, execute_values
from psycopg2.pool import ThreadedConnectionPool

from postmark.reliability import (
    BackoffConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    GracefulShutdown,
    calculate_backoff_delay,
    call_with_circuit_breaker,
    retry_with_backoff,
)

# Service-specific metrics
PUBSUB_MESSAGES = Counter(
    "gmail_sync_pubsub_messages_total",
    "Total Pub/Sub messages processed",
    ["status"],  # success, error
)

PUBSUB_STREAM_RECONNECTS = Counter(
    "gmail_sync_pubsub_stream_reconnects_total",
    "Times the Pub/Sub streaming pull was reopened after it ended",
)

# How long a streaming pull must survive before its next failure counts as a new
# fault rather than a continuing one. Several times the 62.8s median lifetime seen
# in the ACCESS_TOKEN_EXPIRED crash loop, so a rapid-failure pattern escalates
# instead of reconnecting hot.
STREAM_HEALTHY_SECONDS = 300.0

# Ceiling on the reconnect attempt counter. The delay curve saturates at
# max_delay within ~8 attempts, so this changes no timing; it exists because the
# reconnect loop is unbounded and multiplier ** attempt overflows around 1024.
STREAM_BACKOFF_ATTEMPT_CAP = 16

# Consecutive reconnects before the stream is treated as down rather than
# blipping. Below this the failure is logged at WARNING: the process recovers
# unaided, and an ERROR record per blip is how real signal gets buried.
STREAM_FAILURES_BEFORE_ERROR = 5

# How often the backoff wait looks up to see whether a stop was requested.
STOP_POLL_SECONDS = 0.5

# Budget for draining in-flight work at shutdown, and for the stream close that
# follows it. Docker's default stop grace is 10s, so this is already generous.
SHUTDOWN_TIMEOUT_SECONDS = 30.0

# Backoff for reopening the Pub/Sub stream. Slower and longer-lived than the
# per-notification config: a broken stream usually means the network or the
# credential is unavailable, and hammering the subscription helps neither.
# max_retries is unused -- _serve_subscription reconnects for as long as the
# service runs -- but the delay curve is the same shape. Module-level so tests
# assert against the timing the service actually uses rather than a copy.
STREAM_BACKOFF_CONFIG = BackoffConfig(
    initial_delay=2.0,
    max_delay=300.0,
    multiplier=2.0,
    jitter=0.2,
    max_retries=0,
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


# Exceptions worth retrying a notification for, rather than nacking immediately.
#
# OSError is the important entry and it is deliberately broad. This tuple used to
# name urllib.error.URLError and TimeoutError, which look like they cover network
# faults but do not: ssl.SSLError and BrokenPipeError descend from OSError without
# passing through either, so a TLS connection dropped mid-request matched nothing
# here and skipped the backoff entirely. On 2026-09-09 that produced three
# failures inside two seconds, each logged as though it had exhausted its retries.
# Both entries OSError replaces are themselves OSError subclasses, so this is a
# strict widening of that pair rather than a change of intent.
RETRYABLE_NOTIFICATION_EXCEPTIONS: tuple[type[Exception], ...] = (
    HttpError,  # Will be filtered by process_with_error_classification
    psycopg2.Error,
    OSError,  # URLError, TimeoutError, ssl.SSLError, BrokenPipeError, socket.timeout
    RuntimeError,
    CircuitOpenError,
)


logger = get_logger(__name__)


def _partition_name(d: datetime) -> str:
    """Return partition table name for a given date."""
    return f"emails_raw_response_{d.strftime('%Y_%m_%d')}"


def _ensure_partitions(cur: Any, reference_date: datetime | None = None) -> None:
    """Create today's and tomorrow's partitions if they don't exist.

    Also drops partitions older than 7 days.
    """
    now = reference_date or datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Create today and tomorrow partitions
    for offset_days in (0, 1):
        day = today + timedelta(days=offset_days)
        next_day = day + timedelta(days=1)
        name = _partition_name(day)
        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF emails_raw_response FOR VALUES FROM (%s) TO (%s)"
            ).format(sql.Identifier(name)),
            (day.isoformat(), next_day.isoformat()),
        )

    # Drop partitions older than 7 days by querying pg_inherits
    cutoff = today - timedelta(days=7)
    cur.execute(
        """
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'emails_raw_response'
        """
    )
    for (partition_name,) in cur.fetchall():
        try:
            date_str = partition_name.replace("emails_raw_response_", "").replace(
                "_", "-"
            )
            partition_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            if partition_date < cutoff:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(partition_name)
                    )
                )
                logger.info(f"Dropped old partition: {partition_name}")
        except ValueError:
            continue


def _migrate_raw_response(cur: Any) -> None:
    """Migrate raw_response from emails_raw to the partitioned table.

    Runs in batches using a CTE for atomicity — the INSERT and UPDATE
    target the same rows in a single statement. Safe to run multiple
    times and resumable after interruption (uses WHERE raw_response IS NOT NULL).
    Commits after each batch to keep transaction size bounded.
    """
    BATCH_SIZE = 1000

    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM emails_raw WHERE raw_response IS NOT NULL LIMIT 1)"
    )
    has_source = cur.fetchone()[0]
    if not has_source:
        return  # Nothing to migrate

    total_migrated = 0
    logger.info("Migrating raw_response data from emails_raw to emails_raw_response")

    while True:
        cur.execute(
            """
            WITH moved AS (
                SELECT gmail_id, raw_response
                FROM emails_raw
                WHERE raw_response IS NOT NULL
                LIMIT %s
            ),
            inserted AS (
                INSERT INTO emails_raw_response (gmail_id, raw_response, created_at)
                SELECT gmail_id, raw_response, NOW()
                FROM moved
                RETURNING gmail_id
            )
            UPDATE emails_raw
            SET raw_response = NULL
            WHERE gmail_id IN (SELECT gmail_id FROM inserted)
            """,
            (BATCH_SIZE,),
        )
        batch_count = cur.rowcount
        if batch_count == 0:
            break
        cur.connection.commit()
        total_migrated += batch_count
        logger.info(f"Migrated batch of {batch_count} rows ({total_migrated} total)")

    if total_migrated > 0:
        logger.info(
            f"Migration complete: {total_migrated} rows moved to emails_raw_response"
        )


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
    # Enqueue Cortex/* label events to the actions queue for workflow dispatch.
    # Off by default: there is no actions consumer yet, so enabling it only
    # grows the queue unbounded. Flip on when the actions service exists.
    actions_queue_enabled: bool = False

    @classmethod
    def from_env(cls) -> "GmailSyncConfig":
        """Load configuration from environment variables."""
        return cls(
            project_id=os.environ["GCP_PROJECT_ID"],
            subscription_path=os.environ["PUBSUB_SUBSCRIPTION"],
            topic_name=os.environ["PUBSUB_TOPIC"],
            gmail_token_path=os.environ["GMAIL_TOKEN_PATH"],
            duckdb_api_url=os.environ["DUCKDB_API_URL"],
            actions_queue_enabled=os.environ.get("ACTIONS_QUEUE_ENABLED", "").lower()
            in ("1", "true", "yes"),
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

        self._stream_backoff_config = STREAM_BACKOFF_CONFIG

        # The subscription currently being served, so shutdown can cancel it.
        self._streaming_pull_future: Any = None

        # Set by the signal handler and by nothing else; see
        # _install_signal_handlers for why it is a bare bool.
        self._stop_requested = False

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
                        # Return connection to pool
                        self._pool.putconn(conn)
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
        """Ensure the shared queue schema and this service's own tables."""
        # The queue's own schema, from the package that owns it. Not optional
        # and not a manual step: the exponential-backoff feature shipped months
        # before it first RAN in production, its migration existed as a CLI
        # command the deploy flow never invoked, and two workers crash-looped on
        # "column next_attempt_at does not exist" (cortex-rgmk). Idempotent,
        # serialised across services, cheap on the steady-state path.
        ensure_queue_schema(conn)

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
                    claimed_by TEXT,
                    completed_at TIMESTAMPTZ,
                    next_attempt_at TIMESTAMPTZ,
                    CONSTRAINT valid_status CHECK (
                        status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')
                    )
                );

                -- Migration: add next_attempt_at for retry backoff
                ALTER TABLE queue ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

                -- Migration: add claimed_by, the claim token every stale reset
                -- clears and every report in cortex_utils.queue.ops matches on.
                -- The claim statements in parse_worker and attachment_worker
                -- name it unconditionally, so without this a fresh install
                -- claims zero jobs -- the reset is a CTE inside the claim, so
                -- the whole statement aborts rather than degrading.
                ALTER TABLE queue ADD COLUMN IF NOT EXISTS claimed_by TEXT;

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

                -- For attachment: dedup by gmail_id (one job per email)
                -- Drop old index that included attachment_id; new definition drops it
                DROP INDEX IF EXISTS idx_queue_dedup_attachment;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_dedup_attachment
                    ON queue(queue_name, (payload->>'gmail_id'), created_at)
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
                    before_date DATE,
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

                -- Migration: add before_date if missing
                ALTER TABLE backfill_jobs ADD COLUMN IF NOT EXISTS before_date DATE;

                CREATE INDEX IF NOT EXISTS idx_backfill_jobs_pending
                    ON backfill_jobs(status, created_at)
                    WHERE status IN ('pending', 'running');

                -- Partitioned table for raw_response data (moved out of emails_raw)
                CREATE TABLE IF NOT EXISTS emails_raw_response (
                    id BIGSERIAL,
                    gmail_id TEXT NOT NULL,
                    raw_response JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (id, created_at)
                ) PARTITION BY RANGE (created_at);

                CREATE INDEX IF NOT EXISTS idx_raw_response_gmail_id
                    ON emails_raw_response(gmail_id);
            """
            )

            # Ensure today's and tomorrow's partitions exist
            _ensure_partitions(cur)

            # Migrate existing raw_response data if the new table is empty
            _migrate_raw_response(cur)

        # Learning-opportunity table for teach-by-labeling (cortex-uo9b).
        # Does not commit; the caller commits the whole schema block.
        ensure_learning_schema(conn)

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
        """Recursively extract attachment IDs from MIME parts.

        Only includes parts that have both a non-empty filename and an
        attachmentId in the body, filtering out inline content (e.g.
        embedded images) that lack a filename.
        """
        attachment_ids = []

        def recurse(part: dict[str, Any]) -> None:
            body = part.get("body", {})
            if part.get("filename") and body.get("attachmentId"):
                attachment_ids.append(body["attachmentId"])
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
                    # Store in Postgres (raw_response goes to partitioned table only)
                    cur.execute(
                        """
                        INSERT INTO emails_raw (
                            gmail_id, thread_id, history_id, label_ids,
                            internal_date, size_estimate, snippet, headers,
                            mime_type, attachment_ids, raw_response
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
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
                        ),
                    )

                    # Store raw_response in partitioned table (idempotent)
                    cur.execute(
                        """
                        INSERT INTO emails_raw_response (gmail_id, raw_response)
                        SELECT %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM emails_raw_response
                            WHERE gmail_id = %s
                        )
                        """,
                        (gmail_id, json.dumps(self._strip_body(msg)), gmail_id),
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

                    # Enqueue one attachment job per email (idempotent)
                    # Contains all attachment_ids for the message
                    if attachment_ids:
                        cur.execute(
                            """
                            INSERT INTO queue (queue_name, payload, priority)
                            SELECT 'attachment', %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1 FROM queue
                                WHERE queue_name = 'attachment'
                                  AND payload->>'gmail_id' = %s
                                  AND status IN ('pending', 'processing')
                            )
                            """,
                            (
                                Json(
                                    {
                                        "gmail_id": gmail_id,
                                        "attachment_ids": attachment_ids,
                                    }
                                ),
                                priority,
                                gmail_id,
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
        # Gated behind actions_queue_enabled: with no consumer, enqueueing only
        # grows the queue unbounded (see cortex-s8sd).
        actions_to_enqueue = []
        if self.config.actions_queue_enabled:
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

                    if "SPAM" in label_ids:
                        self._enqueue_for_spam_change(conn, gmail_id, event_type)

                    # Bulk insert all actions
                    if actions_to_enqueue:
                        execute_values(
                            cur,
                            "INSERT INTO queue (queue_name, payload) VALUES %s",
                            actions_to_enqueue,
                        )

                # Detect teaching signals (manual label divergence) in the
                # same transaction so they commit atomically with the update.
                self._detect_learning_opportunities(
                    conn, gmail_id, label_ids, event_type
                )

                # Commit the label event transaction
                conn.commit()

            except psycopg2.Error as e:
                conn.rollback()
                logger.error(
                    f"Failed to process label event for {gmail_id}, rolled back: {e}"
                )
                raise

    def _detect_learning_opportunities(
        self,
        conn: psycopg2.extensions.connection,
        gmail_id: str,
        label_ids: list[str],
        event_type: str,
    ) -> None:
        """Record a learning opportunity when a manual label change diverges
        from triage's classification (cortex-uo9b.1).

        A managed-prefix label that a human adds but triage did not assign — or
        removes that triage did assign — is a teaching signal. Machine labels
        always match the classification, so they align and are ignored with no
        attribution ledger. Labels on emails triage has not classified yet are
        noise (gmail-sync sees mail before the user does) and are skipped.
        """
        if not label_ids:
            return

        # Gate each DB query on the cheaper check before it: managed-label
        # filter (cached label names) before the classification lookup, and
        # both before the sender lookup.
        prefix = active_label_prefix(conn)

        def is_managed(name: str) -> bool:
            return not prefix or name.startswith(f"{prefix}/")

        changed = [
            name
            for label_id in label_ids
            if (name := self._get_label_name(label_id)) and is_managed(name)
        ]
        if not changed:
            return  # only non-managed labels changed — not a teaching signal

        if self._is_spam(conn, gmail_id):
            return  # labels on spam are stripped by machine, not taught (cortex-kvyn)

        expected = expected_cortex_labels(conn, gmail_id, prefix=prefix)
        if expected is None:
            return  # not triaged yet -> noise

        # The default label (e.g. Cortex/Uncategorized) is stripped automatically
        # by the labeling worker's reconciliation whenever a real label is
        # applied, so its removal is not a teaching signal — never propose to
        # "stop labeling as <default>". Only needed for removals, so (per the
        # gate-each-query convention above) the lookup is skipped for adds.
        default_full: str | None = None
        if event_type == "removed":
            default = active_default_label(conn)
            default_full = f"{prefix}/{default}" if prefix else default

        def diverges(name: str) -> bool:
            if event_type == "added":
                return name not in expected
            if event_type == "removed":
                # removed a label triage had assigned, except the default label
                return name in expected and name != default_full
            return False

        divergent = [name for name in changed if diverges(name)]
        if not divergent:
            return  # aligned (e.g. machine labeling) — skip the sender lookup

        sender = self._get_sender_addr(conn, gmail_id)
        direction = ADD if event_type == "added" else REMOVE
        for name in divergent:
            if record_learning_opportunity(
                conn,
                gmail_id=gmail_id,
                label=name,
                direction=direction,
                sender=sender,
                expected_labels=expected,
            ):
                logger.info(
                    f"Learning opportunity: {event_type} {name} on {gmail_id} "
                    f"(sender={sender}) diverges from classification"
                )

    def _enqueue_for_spam_change(
        self, conn: psycopg2.extensions.connection, gmail_id: str, event_type: str
    ) -> None:
        """Keep spam out of the pipeline as Gmail moves mail in and out of it.

        Into SPAM: a labeling job, which reconciles the message to no Cortex
        labels. Out of SPAM: a triage job, if the message is parsed -- an
        unparsed one is enqueued for triage by the parse worker (cortex-kvyn).
        Runs in the label event's transaction, so it commits with label_ids.
        """
        if event_type == "added":
            queue = "labeling"
        else:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM emails_parsed WHERE gmail_id = %s", (gmail_id,)
                )
                if cur.fetchone() is None:
                    return
            queue = "triage"
        enqueue(conn, queue, {"gmail_id": gmail_id}, dedup_key="gmail_id", commit=False)

    def _is_spam(self, conn: psycopg2.extensions.connection, gmail_id: str) -> bool:
        """Whether the message currently carries Gmail's SPAM label."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 'SPAM' = ANY(label_ids) FROM emails_raw WHERE gmail_id = %s",
                (gmail_id,),
            )
            row = cur.fetchone()
        return bool(row) and row[0] is True

    def _get_sender_addr(
        self, conn: psycopg2.extensions.connection, gmail_id: str
    ) -> str | None:
        """Look up the parsed sender address for an email, if available."""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT from_addr FROM emails_parsed WHERE gmail_id = %s",
                (gmail_id,),
            )
            row = cur.fetchone()
        return row[0] if row else None

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

        try:
            retry_with_backoff(
                process_with_error_classification,
                config=self._backoff_config,
                retryable_exceptions=RETRYABLE_NOTIFICATION_EXCEPTIONS,
            )
            PUBSUB_MESSAGES.labels(status="success").inc()
        except Exception as e:
            # Either the error was not retryable, or retries were exhausted.
            logger.exception(f"Giving up on notification, nacking for redelivery: {e}")
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
        before: str | None = None,
        job_id: str | None = None,
    ) -> tuple[int, int, int]:
        """Backfill emails from a date range.

        Fetches message IDs via messages.list, stores any missing messages,
        and updates the history baseline if we found newer messages.

        Args:
            days: Number of days back from today (e.g., 7 for last week)
            after: Start date in YYYY-MM-DD format (e.g., "2025-01-01")
            before: End date in YYYY-MM-DD format (e.g., "2025-08-01"), optional
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

        if before:
            try:
                before_date = datetime.strptime(before, "%Y-%m-%d").strftime("%Y/%m/%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format: '{before}'. Expected YYYY-MM-DD (e.g., 2025-08-01)"
                )
            if after_date >= before_date:
                raise ValueError(
                    f"Invalid date range: after ({after_date}) must be before ({before_date})"
                )
            query += f" before:{before_date}"
            logger.info(f"Backfill date range: after {after_date} before {before_date}")

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
                RETURNING id, query, days, after_date, before_date
                """
            )
            row = cur.fetchone()
            conn.commit()

            if not row:
                return False

            job_id, query, days, after_date, before_date = row

        logger.info(f"Starting backfill job {job_id}: {query}")

        try:
            # Run backfill with job tracking
            before_str = before_date.strftime("%Y-%m-%d") if before_date else None
            if after_date:
                processed, stored, updated = self.backfill(
                    after=after_date.strftime("%Y-%m-%d"),
                    before=before_str,
                    job_id=job_id,
                )
            elif days:
                processed, stored, updated = self.backfill(
                    days=days, before=before_str, job_id=job_id
                )
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

    def _safe_maintain_partitions(self) -> None:
        """Create/drop partitions for emails_raw_response. Won't crash scheduler."""
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    _ensure_partitions(cur)
                conn.commit()
            logger.debug("Partition maintenance completed")
        except Exception as e:
            logger.error(f"Error maintaining partitions: {e}", exc_info=True)
            ERRORS.labels(
                service="gmail-sync", error_type="partition_maintenance_failed"
            ).inc()

    def _install_signal_handlers(self) -> None:
        """Make SIGTERM and SIGINT reach the shutdown flag.

        Without this the flag is only ever set by run()'s KeyboardInterrupt
        handler, which runs on the same thread as the reconnect loop and so
        cannot run until that loop has already exited -- leaving its
        should_continue() guards permanently true in production. `docker stop`
        sends SIGTERM, which had no handler at all: the process was killed
        outright, in-flight notifications and all. That mattered less when any
        fault ended the process anyway; now that it stays up across faults it
        needs a real stop.

        The handler does one lock-free store and nothing else. Signal handlers
        run on the main thread, so anything here that takes a lock the main
        thread might already hold deadlocks the process against itself --
        GracefulShutdown.initiate() takes one, and wait_for_completion() holds
        it, which is exactly where a second SIGTERM lands. threading.Event.set()
        has the same hazard. _consume_stop_request turns the flag into the real
        shutdown from ordinary code, where taking a lock is safe.
        """

        def _request_shutdown(signum: int, _frame: Any) -> None:
            if self._stop_requested:
                # Second signal: hand this one back to the default disposition so
                # the next repeat kills the process. A graceful stop that cannot
                # itself be interrupted is a trap -- the flag is only read inside
                # the serve loop, so a signal during startup or during the drain
                # is otherwise swallowed with no way to insist.
                signal.signal(signum, signal.SIG_DFL)
                return
            self._stop_requested = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, _request_shutdown)

    def _consume_stop_request(self) -> bool:
        """Turn a signalled stop into a real one. Returns True if stopping.

        Called from ordinary code rather than from the handler, so it may take
        locks and log.
        """
        if self._stop_requested and self._shutdown.should_continue():
            logger.info("Stop requested, initiating graceful shutdown")
            self._shutdown.initiate()
            streaming_pull_future = self._streaming_pull_future
            if streaming_pull_future is not None:
                with suppress(Exception):
                    streaming_pull_future.cancel()
        return not self._shutdown.should_continue()

    def _await_stream(self, streaming_pull_future: Any) -> None:
        """Block on the stream, waking often enough to notice a stop request.

        The service spends essentially all of its life here, so this is the one
        wait that has to be interruptible. `result()` with no timeout ends in a
        lock acquire, and PEP 475 resumes that after the signal handler returns
        -- the same trap as time.sleep, on the call that matters most. Measured:
        a 2s wait signalled at 0.3s returns at 2.0s. Left unbounded, a stop
        arriving while the stream is healthy is simply never read, and since
        installing a SIGINT handler replaces default_int_handler, Ctrl-C no
        longer raises KeyboardInterrupt either -- so the process would ignore
        both until the stream broke on its own.
        """
        while not self._consume_stop_request():
            try:
                streaming_pull_future.result(timeout=STOP_POLL_SECONDS)
                return
            except FutureTimeoutError:
                if not streaming_pull_future.done():
                    continue
                # Resolved -- and possibly only in the gap between the poll
                # expiring and this check, since result() decides under the
                # condition lock and done() reads after it is released. Re-raising
                # here would report our own poll timeout as the fault that ended
                # the stream, and would turn a clean close into a failure: the
                # reconnect path would log an error and bump the stream-failure
                # counter on the way out of a normal shutdown. Ask the future what
                # actually happened instead; timeout=0 never waits, and a stream
                # that really died of a TimeoutError still re-raises it.
                streaming_pull_future.result(timeout=0)
                return
        # _consume_stop_request cancelled the future; the caller's clean-close
        # path takes it from here.

    def _interruptible_sleep(self, seconds: float) -> None:
        """Wait, but notice a stop request while waiting.

        time.sleep() is the wrong primitive here: since PEP 475 a sleep
        interrupted by a signal is resumed with the remaining time once the
        handler returns normally, so a SIGTERM one second into a saturated
        backoff still waits out the rest. Measured: a 2s sleep signalled at
        0.3s returns at 2.0s. Backoff reaches 300s and `docker stop` gives 10
        before SIGKILL, so that is the difference between a graceful stop and
        never having one.
        """
        deadline = time.monotonic() + seconds
        while not self._stop_requested:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(STOP_POLL_SECONDS, remaining))

    def run(self) -> None:
        """Main entry point - start the service."""
        logger.info("Starting Gmail sync service")

        self._install_signal_handlers()

        # Check watch health and renew on startup
        self._check_watch_health()
        self.renew_watch()

        # Schedule recurring tasks
        # Renew watch every 12 hours (watch expires after 7 days, so plenty of buffer)
        schedule.every(12).hours.do(self._safe_renew_watch)
        schedule.every().hour.do(self.fallback_poll)
        # Check for queued backfill jobs every minute
        schedule.every(1).minutes.do(self._safe_check_backfill_queue)
        # Maintain raw_response partitions daily (create tomorrow's, drop old)
        schedule.every().day.at("00:05").do(self._safe_maintain_partitions)

        # Start scheduler in background thread
        scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()

        try:
            self._serve_subscription()
        except KeyboardInterrupt:
            # Still reachable if an interrupt lands before the handlers are
            # installed, or is raised from somewhere that does not route through
            # the signal handler above.
            logger.info("Shutdown signal received, initiating graceful shutdown...")
            self._stop_requested = True
        finally:
            # Unconditional. _serve_subscription's post-failure block runs
            # outside its own try -- the backoff maths, the structlog call, the
            # counters, _consume_stop_request and the wait -- and that is all
            # error-path code, which runs precisely when something has already
            # gone wrong. An unexpected escape there is exactly when abandoning
            # in-flight notifications costs the most.
            self._consume_stop_request()
            self._drain_and_close()

    def _drain_and_close(self) -> None:
        """Let in-flight notifications finish, then close the stream.

        Reached however shutdown was requested -- a signal, or an interrupt --
        so the drain does not depend on which path got here.
        """
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
        streaming_pull_future = self._streaming_pull_future

        if streaming_pull_future is not None:
            with suppress(Exception):
                streaming_pull_future.cancel()

            # This is the drain, not just the close. cancel() is non-blocking and
            # hands off to a daemon thread that resolves the future only once the
            # callback executor has finished -- and we subscribe asking it to wait
            # for callbacks. Doing it before wait_for_completion matters: messages
            # already handed to that executor enter track_work() after cancel()
            # returns, so a wait_for_completion() run first can sample
            # _in_flight == 0 and report "all work finished" while notifications
            # are still starting.
            #
            # Bounded, because unbounded it would block past any stop grace period
            # on exactly the work the timeout above exists to give up on.
            with suppress(Exception):
                streaming_pull_future.result(timeout=SHUTDOWN_TIMEOUT_SECONDS)

        # Whatever is left of the budget, so the two waits cannot stack into
        # double the grace period we claim to honour.
        remaining = max(deadline - time.monotonic(), 0.0)
        if self._shutdown.wait_for_completion(timeout=remaining):
            logger.info("Graceful shutdown complete - all work finished")
        else:
            logger.warning("Graceful shutdown timeout - some work may be incomplete")
        logger.info("Shutdown complete")

    def _serve_subscription(self) -> None:
        """Hold the Pub/Sub subscription open, reopening it when the stream breaks.

        `.result()` re-raises whatever ended the stream, so an unhandled error
        here ends the process and takes the scheduler and the connection pool
        with it. Reconnects with backoff until shutdown; a stream that stayed up
        longer than STREAM_HEALTHY_SECONDS resets the backoff. Publishes the live
        future on self._streaming_pull_future so the shutdown path can cancel it.
        Returns only once shutdown has been requested.

        See the 2026-09-07/08 ACCESS_TOKEN_EXPIRED restart loop for why this is
        not left to `restart: unless-stopped`.
        """
        # Flow control limits concurrent messages to the connection pool size, so
        # a burst of notifications cannot exhaust it.
        flow_control = FlowControl(max_messages=self.config.postgres.pool_max_conn)
        consecutive_failures = 0

        while not self._consume_stop_request():
            # subscribe() belongs inside the try: opening the stream constructs
            # the streaming-pull manager and starts the bidi RPC, which can fail
            # on its own. Outside, that exception escapes and kills the process,
            # which is the failure this method was written to end.
            streaming_pull_future = None
            started = time.monotonic()
            failure: Exception | None = None

            try:
                streaming_pull_future = self.subscriber.subscribe(
                    self.config.subscription_path,
                    callback=self.process_notification,
                    flow_control=flow_control,
                    # Do not resubscribe over the top of the old stream. This
                    # defaults to False, which shuts the callback executor with
                    # wait=False and resolves the future while callbacks are
                    # still running -- so the next subscribe() below would admit
                    # a second flow-control budget of messages alongside them,
                    # against one connection pool. Worse, those orphaned
                    # callbacks ack into a dispatcher that shutdown already
                    # stopped, so their messages are redelivered and processed
                    # twice, racing on the sync_state checkpoint. Waiting makes
                    # result() returning mean the previous stream is finished.
                    await_callbacks_on_shutdown=True,
                )
                self._streaming_pull_future = streaming_pull_future
                # A signal handler runs on this thread at a bytecode boundary, so
                # a stop requested during subscribe() above cancelled whatever
                # the previous iteration left in _streaming_pull_future -- never
                # this stream, which did not exist yet. Without re-checking here
                # we would block in result() on a healthy stream nobody will
                # cancel, ignoring SIGTERM until the stream broke on its own.
                if self._consume_stop_request():
                    return
                # Restart the clock: uptime measures how long the stream stayed
                # up, and a slow subscribe() must not read as a healthy stream.
                started = time.monotonic()
                logger.info(f"Listening on {self.config.subscription_path}")
                self._await_stream(streaming_pull_future)
            except Exception as e:
                failure = e

            uptime = time.monotonic() - started

            if streaming_pull_future is not None:
                # cancel() tears down the scheduler, leaser and RPC, possibly on
                # a transport that has just failed. Everything else on this path
                # is built so it cannot raise; an exception escaping here would
                # kill the process from inside the code that exists to prevent
                # exactly that.
                with suppress(Exception):
                    streaming_pull_future.cancel()

            # A clean return means the manager closed with no error, which is
            # what cancel() during shutdown looks like. Anything else -- an error,
            # or a clean close while the service is still meant to be serving --
            # goes through the reconnect path below.
            if failure is None and not self._shutdown.should_continue():
                return

            # A stream that ran a good while and then broke is a fresh fault, not
            # an escalating one: hold the backoff for repeated quick failures
            # only, or a nightly blip inherits the previous night's delay.
            if uptime >= STREAM_HEALTHY_SECONDS:
                consecutive_failures = 0
            delay = calculate_backoff_delay(
                consecutive_failures, self._stream_backoff_config
            )
            # Cap the attempt counter, not just the delay. This loop has no retry
            # limit, and calculate_backoff_delay computes multiplier ** attempt
            # before clamping to max_delay -- at attempt 1024 that overflows and
            # raises, which would kill the process from inside the code that
            # exists to keep it alive. The last incident restarted the container
            # 1119 times in two days; an unbounded counter here would see that
            # volume of failures without the restarts to reset it.
            consecutive_failures = min(
                consecutive_failures + 1, STREAM_BACKOFF_ATTEMPT_CAP
            )

            # One reconnect is routine and the process fixes it unaided; ERROR is
            # for something a human should act on. Only once the failures stack
            # up does the stream count as genuinely down rather than blipping.
            # The ERRORS counter fires either way, so alerting does not depend on
            # this level.
            emit = (
                logger.error
                if consecutive_failures >= STREAM_FAILURES_BEFORE_ERROR
                else logger.warning
            )
            # repr, not str: an exception raised bare stringifies to "" and the
            # line would name neither the fault nor its type.
            reason = repr(failure) if failure is not None else "closed without an error"
            emit(
                f"Pub/Sub stream ended after {uptime:.1f}s, "
                f"reconnecting in {delay:.1f}s: {reason}",
                # The exception instance, not True. This runs outside the except
                # block, where sys.exc_info() has already been cleared, so
                # exc_info=True captures nothing and structlog drops it silently
                # -- no traceback, no warning. None is the right no-op for the
                # clean-close branch.
                exc_info=failure,
                # Flat kwargs, not extra=: structlog folds kwargs into the event
                # dict, but extra= becomes a key literally named "extra", nesting
                # these below every other field the service emits.
                **self._stream_failure_diagnostics(),
            )
            ERRORS.labels(service="gmail-sync", error_type="pubsub_stream_failed").inc()

            if self._consume_stop_request():
                return
            # Counted after the shutdown check rather than beside ERRORS above,
            # so a failure that ends the loop is not counted as a reconnect. A
            # stop landing during the wait below still over-counts by one.
            PUBSUB_STREAM_RECONNECTS.inc()
            self._interruptible_sleep(delay)

    def _stream_failure_diagnostics(self) -> dict[str, str]:
        """Facts worth having in the log when the stream dies.

        Records how much life the token had left, by our own reckoning, at the
        moment Google rejected it. Google-auth derives `expiry` from the local
        clock (`utcnow() + expires_in`), so this pair cannot measure our clock
        against Google's -- it shows only whether a token we still considered
        valid was refused, which is the fact that was missing while
        ACCESS_TOKEN_EXPIRED was arriving on tokens a minute old. Establishing
        the cause needs a timestamp from Google's side and is not attempted here.

        Best-effort by construction: this runs on an error path and must not
        raise, and it reads a private attribute that may move between library
        versions.
        """
        now = datetime.now(timezone.utc)
        facts = {"local_utc": now.isoformat()}
        try:
            credentials = self.subscriber.transport._credentials  # type: ignore[union-attr]
            expiry = getattr(credentials, "expiry", None)
            if expiry is None:
                facts["token_expiry_utc"] = "none"
            else:
                # google-auth stores expiry as a naive UTC datetime.
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                facts["token_expiry_utc"] = expiry.isoformat()
                facts["token_seconds_remaining"] = (
                    f"{(expiry - now).total_seconds():.1f}"
                )
        except Exception as e:  # noqa: BLE001 - must never mask the fault
            # Keep the reason: the attribute read above is private and may move
            # between library versions, and without this the field reads
            # "unavailable" forever with no way to tell that from a missing token.
            facts["token_expiry_utc"] = f"unavailable: {e!r}"
        return facts


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
    parser.add_argument(
        "--backfill-before",
        type=str,
        metavar="DATE",
        help="End date for backfill (YYYY-MM-DD), works with --backfill, --backfill-only, or --backfill-from",
    )
    args = parser.parse_args()

    # Configure logging
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    configure_logging(service_name="gmail-sync", level=log_level)

    # Start metrics server
    metrics_port = int(os.environ.get("METRICS_PORT", "8000"))
    start_metrics_server(port=metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")

    config = GmailSyncConfig.from_env()
    sync = GmailSync(config)

    # Handle backfill modes
    if args.backfill_from:
        processed, stored, updated = sync.backfill(
            after=args.backfill_from, before=args.backfill_before
        )
        print(
            f"Backfill complete: {processed} processed, {stored} stored, {updated} updated"
        )
        return

    if args.backfill_only:
        processed, stored, updated = sync.backfill(
            days=args.backfill_only, before=args.backfill_before
        )
        print(
            f"Backfill complete: {processed} processed, {stored} stored, {updated} updated"
        )
        return

    if args.backfill:
        sync.backfill(days=args.backfill, before=args.backfill_before)

    sync.run()


if __name__ == "__main__":
    main()
