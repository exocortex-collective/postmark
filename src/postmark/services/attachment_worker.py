#!/usr/bin/env python3
"""Attachment worker - fetches email attachments from Gmail and stores in S3/Minio."""

import argparse
import base64
import hashlib
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import boto3
import psycopg2
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

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
    s3_endpoint: str
    s3_bucket: str
    gmail_token_path: str
    # Sensitive fields hidden from repr
    postgres_password: str = field(default="", repr=False)
    s3_access_key: str = field(default="", repr=False)
    s3_secret_key: str = field(default="", repr=False)
    batch_size: int = 50
    poll_interval: int = 5


class AttachmentWorker:
    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self._pg_conn: psycopg2.extensions.connection | None = None
        self._s3_client: Any = None  # boto3 S3 client (dynamically typed)
        self._gmail_service: Resource | None = None

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

    def _get_s3_client(self) -> Any:
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.config.s3_endpoint,
                aws_access_key_id=self.config.s3_access_key,
                aws_secret_access_key=self.config.s3_secret_key,
                config=BotoConfig(signature_version="s3v4"),
            )
            # Ensure bucket exists
            try:
                self._s3_client.head_bucket(Bucket=self.config.s3_bucket)
            except ClientError as e:
                # 404 means bucket doesn't exist, other errors should propagate
                if e.response.get("Error", {}).get("Code") == "404":
                    logger.info(f"Creating bucket: {self.config.s3_bucket}")
                    self._s3_client.create_bucket(Bucket=self.config.s3_bucket)
                else:
                    raise
        return self._s3_client

    def _get_gmail_service(self) -> Resource:
        if self._gmail_service is None:
            try:
                with open(self.config.gmail_token_path) as f:
                    token_data = json.load(f)
            except FileNotFoundError:
                logger.critical(
                    f"Gmail token file not found at: {self.config.gmail_token_path}"
                )
                raise
            creds = Credentials(
                token=token_data["token"],
                refresh_token=token_data["refresh_token"],
                token_uri=token_data["token_uri"],
                client_id=token_data["client_id"],
                client_secret=token_data["client_secret"],
                scopes=token_data["scopes"],
            )
            self._gmail_service = build("gmail", "v1", credentials=creds)
        return self._gmail_service

    def _ensure_schema(self) -> None:
        """Ensure attachments table exists."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS attachments (
                    id BIGSERIAL PRIMARY KEY,
                    gmail_id TEXT NOT NULL,
                    attachment_id TEXT NOT NULL,
                    filename TEXT,
                    mime_type TEXT,
                    size_bytes INT,
                    s3_bucket TEXT NOT NULL,
                    s3_key TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(gmail_id, attachment_id)
                )
            """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_attachments_gmail_id
                ON attachments(gmail_id)
            """
            )
            conn.commit()
        logger.info("attachments schema ensured")

    def _claim_jobs(self, batch_size: int) -> list[dict[str, Any]]:
        """Claim a batch of pending attachment jobs."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE queue
                SET status = 'processing', claimed_at = NOW()
                WHERE id IN (
                    SELECT id FROM queue
                    WHERE queue_name = 'attachment' AND status = 'pending'
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

    def _get_attachment_metadata(
        self, gmail_id: str, attachment_id: str
    ) -> dict[str, Any] | None:
        """Get attachment metadata from emails_raw."""
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT raw_response FROM emails_raw WHERE gmail_id = %s",
                (gmail_id,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return None

            # Find attachment in parts
            raw_response = row[0]
            return self._find_attachment_in_parts(
                raw_response.get("payload", {}), attachment_id
            )

    def _find_attachment_in_parts(
        self, payload: dict[str, Any], attachment_id: str
    ) -> dict[str, Any] | None:
        """Recursively find attachment metadata in MIME parts."""
        body = payload.get("body", {})
        if body.get("attachmentId") == attachment_id:
            return {
                "filename": payload.get("filename", "attachment"),
                "mime_type": payload.get("mimeType", "application/octet-stream"),
                "size": body.get("size", 0),
            }

        for part in payload.get("parts", []):
            result = self._find_attachment_in_parts(part, attachment_id)
            if result:
                return result

        return None

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
            )
            conn.commit()

    def process_job(self, job: dict[str, Any]) -> bool:
        """Process a single attachment job. Returns True on success."""
        gmail_id = job["payload"]["gmail_id"]
        attachment_id = job["payload"]["attachment_id"]

        try:
            # Check if already processed
            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM attachments WHERE gmail_id = %s AND attachment_id = %s",
                    (gmail_id, attachment_id),
                )
                if cur.fetchone():
                    logger.debug(
                        f"Attachment already exists: {gmail_id}/{attachment_id}"
                    )
                    self._complete_job(job["id"])
                    return True

            # Get metadata
            metadata = self._get_attachment_metadata(gmail_id, attachment_id)
            if not metadata:
                logger.warning(
                    f"Attachment metadata not found: {gmail_id}/{attachment_id}"
                )
                self._fail_job(
                    job["id"], "Attachment metadata not found in raw_response"
                )
                return False

            # Fetch attachment data from Gmail
            gmail = self._get_gmail_service()
            att_response = (
                gmail.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=gmail_id, id=attachment_id)
                .execute()
            )

            # Decode base64url data
            # Gmail API returns base64url without padding - add correct padding
            raw_data = att_response["data"]
            data = base64.urlsafe_b64decode(raw_data + "=" * (-len(raw_data) % 4))

            # Generate S3 key: attachments/{gmail_id}/{attachment_id_hash}/{filename}
            # Sanitize filename to prevent path traversal attacks
            raw_filename = metadata["filename"] or "attachment"
            filename = os.path.basename(raw_filename)
            # Hash the attachment_id to avoid special characters that Minio rejects
            att_id_hash = hashlib.sha256(attachment_id.encode()).hexdigest()
            s3_key = f"attachments/{gmail_id}/{att_id_hash}/{filename}"

            # Upload to S3/Minio
            s3 = self._get_s3_client()
            s3.put_object(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                Body=data,
                ContentType=metadata["mime_type"],
            )

            # Store record and complete job in single transaction for atomicity
            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO attachments (
                        gmail_id, attachment_id, filename, mime_type,
                        size_bytes, s3_bucket, s3_key
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (gmail_id, attachment_id) DO UPDATE SET
                        filename = EXCLUDED.filename,
                        mime_type = EXCLUDED.mime_type,
                        size_bytes = EXCLUDED.size_bytes,
                        s3_bucket = EXCLUDED.s3_bucket,
                        s3_key = EXCLUDED.s3_key
                """,
                    (
                        gmail_id,
                        attachment_id,
                        filename,
                        metadata["mime_type"],
                        len(data),
                        self.config.s3_bucket,
                        s3_key,
                    ),
                )
                cur.execute(
                    """
                    UPDATE queue
                    SET status = 'completed', completed_at = NOW()
                    WHERE id = %s
                """,
                    (job["id"],),
                )
                conn.commit()

            logger.info(f"Stored attachment: {s3_key} ({len(data)} bytes)")
            return True

        except HttpError as e:
            logger.error(f"Gmail API error for {gmail_id}/{attachment_id}: {e}")
            self._fail_job(job["id"], str(e))
            return False
        except Exception as e:
            logger.error(f"Failed to process {gmail_id}/{attachment_id}: {e}")
            self._fail_job(job["id"], str(e))
            return False

    def run(self) -> None:
        """Main processing loop."""
        self._ensure_schema()

        logger.info("Attachment worker started")

        processed = 0
        failed = 0

        while self.running:
            jobs = self._claim_jobs(self.config.batch_size)

            if not jobs:
                logger.debug(f"No jobs, sleeping {self.config.poll_interval}s")
                time.sleep(self.config.poll_interval)
                continue

            logger.info(f"Processing batch of {len(jobs)} attachments")

            for job in jobs:
                if self.process_job(job):
                    processed += 1
                else:
                    failed += 1

                if (processed + failed) % 50 == 0:
                    logger.info(f"Progress: {processed} processed, {failed} failed")

        logger.info(
            f"Attachment worker stopped: {processed} processed, {failed} failed"
        )

    def run_once(self) -> tuple[int, int]:
        """Process all pending jobs once, then exit."""
        self._ensure_schema()

        logger.info("Attachment worker running once")

        processed = 0
        failed = 0

        while True:
            jobs = self._claim_jobs(self.config.batch_size)

            if not jobs:
                break

            logger.info(f"Processing batch of {len(jobs)} attachments")

            for job in jobs:
                if self.process_job(job):
                    processed += 1
                else:
                    failed += 1

                if (processed + failed) % 50 == 0:
                    logger.info(f"Progress: {processed} processed, {failed} failed")

        logger.info(
            f"Attachment worker complete: {processed} processed, {failed} failed"
        )
        return processed, failed

    def stop(self) -> None:
        """Signal the worker to stop."""
        self.running = False


def main() -> None:
    parser = argparse.ArgumentParser(description="Attachment worker service")
    parser.add_argument(
        "--once", action="store_true", help="Process all pending jobs once and exit"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of jobs to process per batch (default: 50)",
    )
    args = parser.parse_args()

    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "S3_ENDPOINT",
        "S3_BUCKET",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "GMAIL_TOKEN_PATH",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    config = Config(
        postgres_host=os.environ["POSTGRES_HOST"],
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        postgres_db=os.environ["POSTGRES_DB"],
        postgres_user=os.environ["POSTGRES_USER"],
        postgres_password=os.environ["POSTGRES_PASSWORD"],
        s3_endpoint=os.environ["S3_ENDPOINT"],
        s3_bucket=os.environ["S3_BUCKET"],
        s3_access_key=os.environ["S3_ACCESS_KEY"],
        s3_secret_key=os.environ["S3_SECRET_KEY"],
        gmail_token_path=os.environ["GMAIL_TOKEN_PATH"],
        batch_size=args.batch_size,
    )

    worker = AttachmentWorker(config)

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
