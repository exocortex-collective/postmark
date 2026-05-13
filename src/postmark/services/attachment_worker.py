#!/usr/bin/env python3
"""Attachment worker - fetches email attachments from Gmail and stores in S3/Minio."""

import argparse
import base64
import hashlib
import io
import json
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
from cortex_utils.logging import configure_logging, get_logger
from cortex_utils.metrics import (
    ERRORS,
    PROCESSING_DURATION,
    QUEUE_PROCESSED,
    start_metrics_server,
)
from cortex_utils.queue.retry import fail_or_retry, ready_predicate
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from postmark.reliability import GracefulShutdown

logger = get_logger(__name__)

# How long a job can be in 'processing' before it's considered stale
# and reset to 'pending' (visibility timeout)
VISIBILITY_TIMEOUT_MINUTES = 5
MAX_ATTEMPTS = 3

# S3 multipart upload threshold (use multipart for files > 10MB)
# AWS best practice is 10MB, reduces memory usage for large files
MULTIPART_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10MB

# Download chunk size for streaming (1MB chunks)
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB

# S3 multipart upload part size (5MB minimum per AWS requirements)
MULTIPART_PART_SIZE = 5 * 1024 * 1024  # 5MB


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
        self._shutdown = GracefulShutdown()
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
                    logger.info("Creating S3 bucket", bucket=self.config.s3_bucket)
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
                    "Gmail token file not found",
                    token_path=self.config.gmail_token_path,
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
        logger.info("Attachments schema ensured")

    def _claim_jobs(self, batch_size: int) -> list[dict[str, Any]]:
        """Claim a batch of pending attachment jobs.

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
                    WHERE queue_name = 'attachment'
                      AND status = 'processing'
                      AND claimed_at < NOW() - (INTERVAL '1 minute' * %s)
                ),
                claimable AS (
                    SELECT id FROM queue
                    WHERE queue_name = 'attachment' AND status = 'pending'
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

    def _get_attachment_metadata(
        self, gmail_id: str, attachment_id: str
    ) -> dict[str, Any] | None:
        """Get attachment metadata from emails_raw_response (or Gmail API fallback).

        Tries the partitioned emails_raw_response table first.
        Falls back to emails_raw for pre-migration data.
        Falls back to Gmail API if neither has the data.
        """
        conn = self._get_pg_conn()
        raw_response = None

        # Try new partitioned table first
        with conn.cursor() as cur:
            cur.execute(
                "SELECT raw_response, created_at FROM emails_raw_response WHERE gmail_id = %s LIMIT 1",
                (gmail_id,),
            )
            row = cur.fetchone()
            if row and row[0]:
                raw_response = row[0]
                # Cache created_at for partition-pruned DELETE later
                if not hasattr(self, "_raw_response_created_at"):
                    self._raw_response_created_at = {}
                self._raw_response_created_at[gmail_id] = row[1]

        # Fallback: try legacy emails_raw column
        if raw_response is None:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT raw_response FROM emails_raw WHERE gmail_id = %s",
                    (gmail_id,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    raw_response = row[0]

        # Fallback: fetch from Gmail API directly
        if raw_response is None:
            logger.info(
                "raw_response not in DB, fetching from Gmail API",
                gmail_id=gmail_id,
            )
            try:
                gmail = self._get_gmail_service()
                raw_response = (
                    gmail.users()
                    .messages()
                    .get(userId="me", id=gmail_id, format="full")
                    .execute()
                )
            except Exception as e:
                logger.error(
                    "Failed to fetch message from Gmail API",
                    gmail_id=gmail_id,
                    error=str(e),
                )
                return None

        # Recursively search all nested MIME parts for the attachment
        return self._find_attachment_in_parts(
            raw_response.get("payload", {}), attachment_id
        )

    def _find_attachment_in_parts(
        self, payload: dict[str, Any], attachment_id: str
    ) -> dict[str, Any] | None:
        """Recursively find attachment metadata in MIME parts.

        Searches the entire MIME tree, including deeply nested multipart
        structures (e.g., multipart/mixed containing multipart/alternative
        with attachments at various levels).
        """
        body = payload.get("body", {})
        if payload.get("filename") and body.get("attachmentId") == attachment_id:
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

    def _delete_raw_response(self, gmail_id: str) -> None:
        """Delete raw_response from partitioned table after attachment processing."""
        conn = self._get_pg_conn()
        created_at = getattr(self, "_raw_response_created_at", {}).pop(gmail_id, None)
        with conn.cursor() as cur:
            if created_at:
                # Partition-pruned delete (targets specific partition)
                cur.execute(
                    "DELETE FROM emails_raw_response WHERE gmail_id = %s AND created_at = %s",
                    (gmail_id, created_at),
                )
            else:
                # Fallback: scan all partitions
                cur.execute(
                    "DELETE FROM emails_raw_response WHERE gmail_id = %s",
                    (gmail_id,),
                )
            if cur.rowcount > 0:
                logger.debug(
                    "Deleted raw_response after processing",
                    gmail_id=gmail_id,
                    rows_deleted=cur.rowcount,
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
        conn.rollback()
        fail_or_retry(conn, job_id, error, max_attempts=MAX_ATTEMPTS)
        conn.commit()

    def _upload_small_file(self, s3_key: str, data: bytes, mime_type: str) -> None:
        """Upload small file using simple put_object."""
        s3 = self._get_s3_client()
        s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=s3_key,
            Body=data,
            ContentType=mime_type,
        )

    def _upload_small_file_direct(
        self, s3_key: str, gmail_id: str, attachment_id: str, mime_type: str
    ) -> int:
        """Fetch and upload small file directly (non-streaming).

        Returns the actual bytes uploaded.
        """
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

        self._upload_small_file(s3_key, data, mime_type)
        return len(data)

    def _upload_large_file_streaming(
        self, s3_key: str, gmail_id: str, attachment_id: str, mime_type: str
    ) -> int:
        """Stream large file from Gmail to S3 using multipart upload.

        Returns the total bytes uploaded.
        """
        s3 = self._get_s3_client()
        gmail = self._get_gmail_service()

        upload_id = None
        try:
            # Initiate multipart upload
            multipart = s3.create_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                ContentType=mime_type,
            )
            upload_id = multipart["UploadId"]

            # Get attachment as media request for streaming
            request = (
                gmail.users()
                .messages()
                .attachments()
                .get_media(userId="me", messageId=gmail_id, id=attachment_id)
            )

            # Stream download in chunks using context managers for proper cleanup
            with io.BytesIO() as buffer:
                downloader = MediaIoBaseDownload(
                    buffer, request, chunksize=DOWNLOAD_CHUNK_SIZE
                )

                parts = []
                part_number = 1
                total_bytes = 0
                with io.BytesIO() as part_buffer:
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()

                        # Read downloaded chunk from buffer
                        buffer.seek(0)
                        chunk_data = buffer.read()
                        buffer.seek(0)
                        buffer.truncate(0)

                        # Accumulate chunks into parts for multipart upload
                        part_buffer.write(chunk_data)
                        total_bytes += len(chunk_data)

                        # Upload when we have enough data or this is the last chunk
                        if part_buffer.tell() >= MULTIPART_PART_SIZE or done:
                            if part_buffer.tell() > 0:
                                # Pass BytesIO directly to avoid memory copy from getvalue()
                                part_buffer.seek(0)
                                response = s3.upload_part(
                                    Bucket=self.config.s3_bucket,
                                    Key=s3_key,
                                    PartNumber=part_number,
                                    UploadId=upload_id,
                                    Body=part_buffer,
                                )
                                parts.append(
                                    {
                                        "PartNumber": part_number,
                                        "ETag": response["ETag"],
                                    }
                                )
                                part_number += 1

                            # Reset part buffer for next part, avoiding reallocation
                            part_buffer.seek(0)
                            part_buffer.truncate(0)

                    # Complete multipart upload
                    s3.complete_multipart_upload(
                        Bucket=self.config.s3_bucket,
                        Key=s3_key,
                        UploadId=upload_id,
                        MultipartUpload={"Parts": parts},
                    )

            return total_bytes

        except Exception as e:
            # Abort multipart upload on error to prevent orphaned parts
            if upload_id:
                logger.warning(
                    "Aborting multipart upload due to error",
                    s3_key=s3_key,
                    upload_id=upload_id,
                    error=str(e),
                )
                try:
                    s3.abort_multipart_upload(
                        Bucket=self.config.s3_bucket,
                        Key=s3_key,
                        UploadId=upload_id,
                    )
                except Exception as abort_exc:
                    logger.error(
                        "Failed to abort multipart upload - orphaned parts may remain",
                        s3_key=s3_key,
                        upload_id=upload_id,
                        abort_error=str(abort_exc),
                    )
            # Always re-raise the original exception
            raise

    def _process_single_attachment(
        self, gmail_id: str, attachment_id: str
    ) -> tuple[str, str, str, int]:
        """Fetch and upload a single attachment. Returns (attachment_id, filename, mime_type, size)."""
        # Get metadata
        metadata = self._get_attachment_metadata(gmail_id, attachment_id)
        if not metadata:
            raise ValueError(
                f"Attachment metadata not found in raw_response for {attachment_id}"
            )

        # Generate S3 key: attachments/{gmail_id}/{attachment_id_hash}/{filename}
        # Sanitize filename to prevent path traversal attacks
        raw_filename = metadata["filename"] or "attachment"
        filename = os.path.basename(raw_filename)
        # Hash the attachment_id to avoid special characters that Minio rejects
        att_id_hash = hashlib.sha256(attachment_id.encode()).hexdigest()
        s3_key = f"attachments/{gmail_id}/{att_id_hash}/{filename}"

        # Choose upload strategy based on size
        size_bytes = metadata["size"]
        if size_bytes < MULTIPART_THRESHOLD_BYTES:
            actual_size = self._upload_small_file_direct(
                s3_key, gmail_id, attachment_id, metadata["mime_type"]
            )
            if actual_size != size_bytes:
                logger.warning(
                    "Attachment size mismatch for small file",
                    s3_key=s3_key,
                    expected_size=size_bytes,
                    actual_size=actual_size,
                    gmail_id=gmail_id,
                )
            logger.info(
                "Stored small attachment",
                s3_key=s3_key,
                size_bytes=actual_size,
                gmail_id=gmail_id,
            )
        else:
            logger.info(
                "Streaming large attachment",
                s3_key=s3_key,
                size_bytes=size_bytes,
                gmail_id=gmail_id,
            )
            actual_size = self._upload_large_file_streaming(
                s3_key, gmail_id, attachment_id, metadata["mime_type"]
            )
            if actual_size != size_bytes:
                logger.warning(
                    "Attachment size mismatch for large file",
                    s3_key=s3_key,
                    expected_size=size_bytes,
                    actual_size=actual_size,
                    gmail_id=gmail_id,
                )
            logger.info(
                "Stored large attachment",
                s3_key=s3_key,
                size_bytes=actual_size,
                gmail_id=gmail_id,
            )

        return attachment_id, filename, metadata["mime_type"], actual_size

    def process_job(self, job: dict[str, Any]) -> bool:
        """Process a single attachment job. Returns True on success.

        Each job contains a gmail_id and a list of attachment_ids for that
        email.  Supports both the new format (attachment_ids: list) and the
        legacy format (attachment_id: str) for backwards compatibility with
        queue entries created before this change.
        """
        gmail_id = job["payload"]["gmail_id"]
        # Support both new (attachment_ids list) and legacy (single attachment_id) payload
        attachment_ids: list[str] = job["payload"].get("attachment_ids", [])
        if not attachment_ids and "attachment_id" in job["payload"]:
            attachment_ids = [job["payload"]["attachment_id"]]
        start_time = time.time()

        try:
            conn = self._get_pg_conn()
            # Filter out already-processed attachments
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT attachment_id FROM attachments WHERE gmail_id = %s AND attachment_id = ANY(%s)",
                    (gmail_id, attachment_ids),
                )
                existing = {row[0] for row in cur.fetchall()}

            remaining = [aid for aid in attachment_ids if aid not in existing]
            if existing:
                logger.debug(
                    "Skipping already-processed attachments",
                    gmail_id=gmail_id,
                    skipped=len(existing),
                )

            if not remaining:
                logger.debug(
                    "All attachments already processed",
                    gmail_id=gmail_id,
                )
                self._complete_job(job["id"])
                try:
                    self._delete_raw_response(gmail_id)
                except Exception as e:
                    logger.warning(
                        "Failed to delete raw_response after skip",
                        gmail_id=gmail_id,
                        error=str(e),
                    )
                return True

            # Process each attachment
            results: list[tuple[str, str, str, int]] = []
            for attachment_id in remaining:
                result = self._process_single_attachment(gmail_id, attachment_id)
                results.append(result)

            # Store all records and complete job in single transaction
            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                for attachment_id, filename, mime_type, actual_size in results:
                    att_id_hash = hashlib.sha256(attachment_id.encode()).hexdigest()
                    s3_key = f"attachments/{gmail_id}/{att_id_hash}/{filename}"
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
                            mime_type,
                            actual_size,
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

            # Clean up raw_response now that all attachments are processed
            try:
                self._delete_raw_response(gmail_id)
            except Exception as e:
                # Non-fatal: raw_response cleanup failure shouldn't fail the job
                logger.warning(
                    "Failed to delete raw_response after processing",
                    gmail_id=gmail_id,
                    error=str(e),
                )

            # Record processing duration
            PROCESSING_DURATION.labels(queue="attachment", operation="process").observe(
                time.time() - start_time
            )

            return True

        except HttpError as e:
            logger.error(
                "Gmail API error",
                gmail_id=gmail_id,
                error=str(e),
                exc_info=True,
            )
            self._fail_job(job["id"], str(e))
            return False
        except Exception as e:
            logger.error(
                "Failed to process attachments",
                gmail_id=gmail_id,
                error=str(e),
                exc_info=True,
            )
            self._fail_job(job["id"], str(e))
            return False

    def run(self) -> None:
        """Main processing loop with graceful shutdown support."""
        self._ensure_schema()

        logger.info("Attachment worker started")

        processed = 0
        failed = 0

        while self._shutdown.should_continue():
            jobs = self._claim_jobs(self.config.batch_size)

            if not jobs:
                logger.debug(
                    "No jobs, sleeping",
                    poll_interval_seconds=self.config.poll_interval,
                )
                # Check shutdown more frequently than poll interval
                for _ in range(self.config.poll_interval):
                    if not self._shutdown.should_continue():
                        break
                    time.sleep(1)
                continue

            logger.info("Processing batch", batch_size=len(jobs))

            for job in jobs:
                if not self._shutdown.should_continue():
                    logger.info("Shutdown requested, finishing current batch")
                    break

                with self._shutdown.track_work():
                    if self.process_job(job):
                        processed += 1
                        QUEUE_PROCESSED.labels(
                            queue="attachment", status="success"
                        ).inc()
                    else:
                        failed += 1
                        QUEUE_PROCESSED.labels(queue="attachment", status="error").inc()
                        ERRORS.labels(
                            service="attachment-worker", error_type="job_failed"
                        ).inc()

                if (processed + failed) % 50 == 0:
                    logger.info("Progress", processed=processed, failed=failed)

        # Wait for any remaining in-flight work
        self._shutdown.wait_for_completion(timeout=30.0)
        logger.info(
            "Attachment worker stopped",
            total_processed=processed,
            total_failed=failed,
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

            logger.info("Processing batch", batch_size=len(jobs))

            for job in jobs:
                if self.process_job(job):
                    processed += 1
                    QUEUE_PROCESSED.labels(queue="attachment", status="success").inc()
                else:
                    failed += 1
                    QUEUE_PROCESSED.labels(queue="attachment", status="error").inc()
                    ERRORS.labels(
                        service="attachment-worker", error_type="job_failed"
                    ).inc()

                if (processed + failed) % 50 == 0:
                    logger.info("Progress", processed=processed, failed=failed)

        logger.info(
            "Attachment worker complete",
            total_processed=processed,
            total_failed=failed,
        )
        return processed, failed

    def stop(self) -> None:
        """Signal the worker to stop gracefully."""
        logger.info("Initiating graceful shutdown")
        self._shutdown.initiate()


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
        logger.error("Missing required environment variables", variables=missing)
        sys.exit(1)

    # Configure logging
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    configure_logging("attachment-worker", level=log_level)

    # Start metrics server
    metrics_port = int(os.environ.get("METRICS_PORT", "8002"))
    start_metrics_server(port=metrics_port)
    logger.info("Metrics server started", port=metrics_port)

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
