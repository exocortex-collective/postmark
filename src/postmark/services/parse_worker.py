#!/usr/bin/env python3
"""Parse worker service - processes raw emails from queue into structured data."""

import argparse
import base64
import binascii
import json
import logging
import os
import re
import signal
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any

import psycopg2
from psycopg2.extras import Json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    duckdb_api_url: str
    batch_size: int = 100
    poll_interval: int = 5  # seconds between queue polls when empty


@dataclass
class ParsedEmail:
    gmail_id: str
    from_addr: str
    from_name: str
    to_addrs: list[str]
    cc_addrs: list[str]
    bcc_addrs: list[str]
    subject: str
    date_header: datetime | None
    message_id: str
    in_reply_to: str
    references: list[str]
    body_text: str
    body_html: str


def decode_mime_header(header_value: str | None) -> str:
    """Decode RFC 2047 encoded header."""
    if not header_value:
        return ""

    decoded_parts = []
    for part, charset in decode_header(header_value):
        if isinstance(part, bytes):
            charset = charset or "utf-8"
            try:
                decoded_parts.append(part.decode(charset, errors="replace"))
            except (LookupError, UnicodeDecodeError):
                decoded_parts.append(part.decode("utf-8", errors="replace"))
        else:
            decoded_parts.append(part)
    return "".join(decoded_parts)


def extract_email_address(header_value: str | None) -> tuple[str, str]:
    """Extract email address and display name from header.

    Returns (address, name) tuple.
    """
    if not header_value:
        return ("", "")
    decoded = decode_mime_header(header_value)
    name, addr = parseaddr(decoded)
    return (addr or decoded, name)


def extract_email_list(header_value: str | None) -> list[str]:
    """Extract list of email addresses from comma-separated header."""
    if not header_value:
        return []
    decoded = decode_mime_header(header_value)
    # Split on comma, but be careful with quoted names containing commas
    addresses = []
    for part in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', decoded):
        part = part.strip()
        if part:
            _, addr = parseaddr(part)
            addresses.append(addr or part)
    return [a for a in addresses if a]


def parse_date(header_value: str | None, internal_date_ms: int) -> datetime | None:
    """Parse date from header, falling back to internal_date."""
    if header_value:
        try:
            dt = parsedate_to_datetime(header_value)
            # Ensure timezone awareness
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            pass

    # Fall back to Gmail's internal date
    if internal_date_ms:
        return datetime.fromtimestamp(internal_date_ms / 1000, tz=timezone.utc)
    return None


def parse_references(header_value: str | None) -> list[str]:
    """Parse References header into list of message IDs."""
    if not header_value:
        return []
    # Message-IDs are enclosed in angle brackets
    return re.findall(r"<[^>]+>", header_value)


def decode_base64_body(data: str | None) -> str:
    """Decode base64url encoded body data."""
    if not data:
        return ""
    try:
        # Gmail uses URL-safe base64
        decoded = base64.urlsafe_b64decode(data + "==")
        return decoded.decode("utf-8", errors="replace")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return ""


def extract_body_from_parts(parts: list[dict[str, Any]] | None, mime_type: str) -> str:
    """Recursively extract body content from MIME parts."""
    if not parts:
        return ""

    for part in parts:
        part_mime = part.get("mimeType", "")

        if part_mime == mime_type:
            body = part.get("body", {})
            data = body.get("data")
            if data:
                return decode_base64_body(data)

        # Recurse into nested parts
        nested = part.get("parts")
        if nested:
            result = extract_body_from_parts(nested, mime_type)
            if result:
                return result

    return ""


def get_header(headers: list[dict[str, str]], name: str) -> str | None:
    """Get header value by name (case-insensitive)."""
    name_lower = name.lower()
    for h in headers:
        if h.get("name", "").lower() == name_lower:
            return h.get("value")
    return None


def get_body_data(duckdb_api_url: str, gmail_id: str) -> dict[str, Any]:
    """Fetch body data from DuckDB API."""
    url = f"{duckdb_api_url}/body?gmail_id={gmail_id}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return {"parts": data.get("parts"), "body_data": data.get("body_data")}
    except Exception:
        return {"parts": None, "body_data": None}


def parse_email(raw: dict[str, Any], body_data: dict[str, Any]) -> ParsedEmail:
    """Parse a raw email into structured fields."""
    headers = raw["headers"]  # Already JSON from Postgres

    from_addr, from_name = extract_email_address(get_header(headers, "From"))

    to_header = get_header(headers, "To")
    # Fall back to Delivered-To for BCC'd emails or undisclosed-recipients
    if not to_header or "undisclosed-recipients" in (to_header or "").lower():
        to_header = get_header(headers, "Delivered-To")
    to_addrs = extract_email_list(to_header)

    cc_addrs = extract_email_list(get_header(headers, "Cc"))
    bcc_addrs = extract_email_list(get_header(headers, "Bcc"))
    subject = decode_mime_header(get_header(headers, "Subject"))
    date = parse_date(get_header(headers, "Date"), raw["internal_date"])
    message_id = get_header(headers, "Message-ID") or ""
    in_reply_to = get_header(headers, "In-Reply-To") or ""
    references = parse_references(get_header(headers, "References"))

    # Extract body from DuckDB data (API returns parsed JSON)
    parts = body_data.get("parts")
    parts_list = json.loads(parts) if isinstance(parts, str) else parts

    body_text = ""
    body_html = ""

    # Try to get body from parts first (multipart emails)
    if parts_list:
        body_text = extract_body_from_parts(parts_list, "text/plain")
        body_html = extract_body_from_parts(parts_list, "text/html")

    # Fall back to direct body_data if no parts (single-part emails)
    if not body_text and not body_html:
        direct_body = body_data.get("body_data")
        if direct_body:
            decoded = decode_base64_body(direct_body)
            # Use mime_type from raw email to determine body type
            mime_type = raw.get("mime_type", "text/plain")
            if mime_type == "text/html":
                body_html = decoded
            else:
                body_text = decoded

    return ParsedEmail(
        gmail_id=raw["gmail_id"],
        from_addr=from_addr,
        from_name=from_name,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        bcc_addrs=bcc_addrs,
        subject=subject,
        date_header=date,
        message_id=message_id,
        in_reply_to=in_reply_to,
        references=references,
        body_text=body_text,
        body_html=body_html,
    )


class ParseWorker:
    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self._pg_conn: psycopg2.extensions.connection | None = None

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
        """Claim a batch of pending parse jobs."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            # Claim jobs atomically
            cur.execute(
                """
                UPDATE queue
                SET status = 'processing', claimed_at = NOW()
                WHERE id IN (
                    SELECT id FROM queue
                    WHERE queue_name = 'parse' AND status = 'pending'
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, payload
            """,
                (batch_size,),
            )
            jobs = [{"id": row[0], "payload": row[1]} for row in cur.fetchall()]
            conn.commit()
        return jobs

    def _get_raw_email(self, gmail_id: str) -> dict[str, Any] | None:
        """Fetch raw email from Postgres."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT gmail_id, thread_id, headers, internal_date, snippet, mime_type
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
        """Mark job as failed."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE queue
                SET status = 'failed',
                    last_error = %s,
                    attempts = attempts + 1
                WHERE id = %s
            """,
                (error[:1000], job_id),
            )  # Truncate error to 1000 chars
            conn.commit()

    def _enqueue_classify(self, gmail_id: str) -> None:
        """Enqueue email for classification."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO queue (queue_name, payload)
                VALUES ('classify', %s)
            """,
                (Json({"gmail_id": gmail_id}),),
            )
            conn.commit()

    def process_job(self, job: dict[str, Any]) -> bool:
        """Process a single parse job. Returns True on success."""
        gmail_id = job["payload"]["gmail_id"]

        try:
            # Get raw email
            raw = self._get_raw_email(gmail_id)
            if not raw:
                logger.warning(f"Raw email not found: {gmail_id}")
                self._fail_job(job["id"], "Raw email not found")
                return False

            # Get body data from DuckDB API
            body_data = get_body_data(self.config.duckdb_api_url, gmail_id)

            # Parse email
            parsed = parse_email(raw, body_data)

            # Store parsed result
            self._store_parsed(parsed)

            # Enqueue for classification
            self._enqueue_classify(gmail_id)

            # Mark job complete
            self._complete_job(job["id"])

            return True

        except Exception as e:
            logger.error(f"Failed to parse {gmail_id}: {e}")
            self._fail_job(job["id"], str(e))
            return False

    def run(self) -> None:
        """Main processing loop."""
        self._ensure_schema()

        logger.info("Parse worker started")

        processed = 0
        failed = 0

        while True:
            if not self.running:
                break

            jobs = self._claim_jobs(self.config.batch_size)

            if not jobs:
                logger.debug(f"No jobs, sleeping {self.config.poll_interval}s")
                time.sleep(self.config.poll_interval)
                continue

            logger.info(f"Processing batch of {len(jobs)} jobs")

            for job in jobs:
                if self.process_job(job):
                    processed += 1
                else:
                    failed += 1

                if (processed + failed) % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {failed} failed")

        logger.info(f"Parse worker stopped: {processed} processed, {failed} failed")

    def run_once(self) -> tuple[int, int]:
        """Process all pending jobs once, then exit. Returns (processed, failed)."""
        self._ensure_schema()

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
                else:
                    failed += 1

                if (processed + failed) % 100 == 0:
                    logger.info(f"Progress: {processed} processed, {failed} failed")

        logger.info(f"Parse worker complete: {processed} processed, {failed} failed")
        return processed, failed

    def stop(self) -> None:
        """Signal the worker to stop."""
        self.running = False


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

    # All config from environment - use .env.template as reference
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "DUCKDB_API_URL",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error("See .env.template for required configuration")
        sys.exit(1)

    config = Config(
        postgres_host=os.environ["POSTGRES_HOST"],
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        duckdb_api_url=os.environ["DUCKDB_API_URL"],
        batch_size=args.batch_size,
    )

    worker = ParseWorker(config)

    # Handle signals for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        logger.info("Received shutdown signal")
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
