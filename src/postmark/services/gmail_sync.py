"""Gmail sync service - pulls from Pub/Sub, stores raw messages."""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb
import psycopg2
import schedule
from google.cloud import pubsub_v1
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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


@dataclass
class GmailSyncConfig:
    """Configuration for GmailSync service."""

    project_id: str
    subscription_path: str
    topic_name: str
    gmail_token_path: str
    duckdb_path: str
    postgres: PostgresConfig

    @classmethod
    def from_env(cls) -> "GmailSyncConfig":
        """Load configuration from environment variables."""
        return cls(
            project_id=os.environ["GCP_PROJECT_ID"],
            subscription_path=os.environ["PUBSUB_SUBSCRIPTION"],
            topic_name=os.environ["PUBSUB_TOPIC"],
            gmail_token_path=os.environ["GMAIL_TOKEN_PATH"],
            duckdb_path=os.environ["DUCKDB_PATH"],
            postgres=PostgresConfig(
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ.get("POSTGRES_PORT", "5432")),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            ),
        )


class GmailSync:
    """Main sync service."""

    def __init__(self, config: GmailSyncConfig) -> None:
        self.config = config

        # Initialize clients
        self._init_gmail()
        self._init_pubsub()
        self._init_postgres()
        self._init_duckdb()

    def _init_gmail(self) -> None:
        """Initialize Gmail API client."""
        with open(self.config.gmail_token_path) as f:
            token_data = json.load(f)

        creds = Credentials(
            token=token_data["token"],
            refresh_token=token_data["refresh_token"],
            token_uri=token_data["token_uri"],
            client_id=token_data["client_id"],
            client_secret=token_data["client_secret"],
            scopes=token_data["scopes"],
        )
        self.gmail = build("gmail", "v1", credentials=creds)
        logger.info("Gmail client initialized")

    def _init_pubsub(self) -> None:
        """Initialize Pub/Sub subscriber."""
        self.subscriber = pubsub_v1.SubscriberClient()
        logger.info(
            f"Pub/Sub subscriber initialized for {self.config.subscription_path}"
        )

    def _init_postgres(self) -> None:
        """Initialize Postgres connection and ensure schema exists."""
        pg = self.config.postgres
        self.pg_conn = psycopg2.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.dbname,
            user=pg.user,
            password=pg.password,
        )
        self.pg_conn.autocommit = True
        self._ensure_postgres_schema()
        logger.info("Postgres connection initialized")

    def _ensure_postgres_schema(self) -> None:
        """Create tables if they don't exist."""
        with self.pg_conn.cursor() as cur:
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
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

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
                    attempts INT DEFAULT 0,
                    max_attempts INT DEFAULT 3,
                    last_error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ,
                    CONSTRAINT valid_status CHECK (
                        status IN ('pending', 'processing', 'completed', 'failed')
                    )
                );

                CREATE INDEX IF NOT EXISTS idx_queue_pending
                    ON queue(queue_name, status, created_at)
                    WHERE status = 'pending';
            """
            )
        logger.info("Postgres schema ensured")

    def _init_duckdb(self) -> None:
        """Initialize DuckDB and ensure schema exists."""
        self.duckdb_conn = duckdb.connect(self.config.duckdb_path)
        self.duckdb_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_bodies (
                gmail_id TEXT PRIMARY KEY,
                parts JSON,
                body_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        logger.info(f"DuckDB initialized at {self.config.duckdb_path}")

    def _get_last_history_id(self, email_address: str) -> str | None:
        """Get last synced history ID for an email address."""
        with self.pg_conn.cursor() as cur:
            cur.execute(
                "SELECT last_history_id FROM sync_state WHERE email_address = %s",
                (email_address,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _set_last_history_id(self, email_address: str, history_id: str) -> None:
        """Update last synced history ID."""
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_state (email_address, last_history_id, last_sync_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (email_address)
                DO UPDATE SET last_history_id = %s, last_sync_at = NOW()
                """,
                (email_address, history_id, history_id),
            )

    def _email_exists(self, gmail_id: str) -> bool:
        """Check if email already exists in database."""
        with self.pg_conn.cursor() as cur:
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

    def store_raw_message(self, gmail_id: str) -> None:
        """Fetch and store raw message - no parsing, just slam it in."""
        if self._email_exists(gmail_id):
            logger.debug(f"Message {gmail_id} already exists, skipping")
            return

        msg = (
            self.gmail.users()
            .messages()
            .get(userId="me", id=gmail_id, format="full")
            .execute()
        )

        payload = msg.get("payload", {})
        attachment_ids = self._extract_attachment_ids(payload)

        # Store in Postgres
        with self.pg_conn.cursor() as cur:
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

        # Store body in DuckDB
        body_data = payload.get("body", {}).get("data")
        parts = payload.get("parts")
        self.duckdb_conn.execute(
            """
            INSERT OR REPLACE INTO email_bodies (gmail_id, parts, body_data)
            VALUES (?, ?, ?)
            """,
            (gmail_id, json.dumps(parts) if parts else None, body_data),
        )

        # Enqueue for parsing
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO queue (queue_name, payload)
                VALUES ('parse', %s)
                """,
                (json.dumps({"gmail_id": gmail_id}),),
            )

        # Enqueue attachments for separate fetch
        for att_id in attachment_ids:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO queue (queue_name, payload)
                    VALUES ('attachment', %s)
                    """,
                    (json.dumps({"gmail_id": gmail_id, "attachment_id": att_id}),),
                )

        logger.info(f"Stored message {gmail_id}")

    def process_notification(
        self, message: pubsub_v1.subscriber.message.Message
    ) -> None:
        """Handle a single Pub/Sub notification."""
        try:
            data = json.loads(message.data.decode("utf-8"))
            email_address = data["emailAddress"]
            new_history_id = data["historyId"]

            logger.info(f"Notification for {email_address}, historyId={new_history_id}")

            last_history_id = self._get_last_history_id(email_address)

            if last_history_id is None:
                # First sync - just record the history ID, don't backfill
                logger.info(
                    f"First notification for {email_address}, recording baseline"
                )
                self._set_last_history_id(email_address, new_history_id)
                message.ack()
                return

            # Fetch history
            try:
                history_response = (
                    self.gmail.users()
                    .history()
                    .list(
                        userId="me",
                        startHistoryId=last_history_id,
                        historyTypes=["messageAdded"],
                    )
                    .execute()
                )
            except HttpError as e:
                if e.resp.status == 404:
                    # History too old, record new baseline
                    logger.warning(
                        f"History {last_history_id} too old, resetting to {new_history_id}"
                    )
                    self._set_last_history_id(email_address, new_history_id)
                    message.ack()
                    return
                raise

            # Process new messages
            for record in history_response.get("history", []):
                for msg_added in record.get("messagesAdded", []):
                    msg_id = msg_added["message"]["id"]
                    self.store_raw_message(msg_id)

            # Update history ID
            new_id = history_response.get("historyId", new_history_id)
            self._set_last_history_id(email_address, new_id)
            message.ack()

        except Exception as e:
            logger.exception(f"Error processing notification: {e}")
            message.nack()

    def _get_current_user_email(self) -> str:
        """Get the email address of the authenticated user."""
        profile = self.gmail.users().getProfile(userId="me").execute()
        return str(profile["emailAddress"])

    def renew_watch(self) -> None:
        """Renew Gmail push notification registration."""
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
            expiration_dt = datetime.fromtimestamp(expiration_ms / 1000)
            history_id = str(response["historyId"])

            # Get the actual email address for this user
            email_address = self._get_current_user_email()

            with self.pg_conn.cursor() as cur:
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

            logger.info(
                f"Watch renewed for {email_address}, expires at {expiration_dt}"
            )

        except Exception as e:
            logger.exception(f"Failed to renew watch: {e}")

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

    def run(self) -> None:
        """Main entry point - start the service."""
        logger.info("Starting Gmail sync service")

        # Renew watch on startup
        self.renew_watch()

        # Schedule recurring tasks
        schedule.every().day.at("00:00").do(self.renew_watch)
        schedule.every().hour.do(self.fallback_poll)

        # Start scheduler in background thread
        scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()

        # Start Pub/Sub subscriber (blocks)
        streaming_pull_future = self.subscriber.subscribe(
            self.config.subscription_path, callback=self.process_notification
        )
        logger.info(f"Listening on {self.config.subscription_path}")

        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
            logger.info("Shutdown complete")


def main() -> None:
    """Entry point."""
    config = GmailSyncConfig.from_env()
    sync = GmailSync(config)
    sync.run()


if __name__ == "__main__":
    main()
