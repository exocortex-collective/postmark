import os
from pathlib import Path
from typing import Optional

import click
import indexed_gzip as igz
from dotenv import load_dotenv

from postmark.email.dedup import BloomFilter
from postmark.email.processing import extract_email_blobs, process_email
from postmark.storage.columnar import ColumnarWriter
from postmark.storage.s3 import S3Client
from postmark.storage.sqlite import create_connection, create_schema


@click.command()
@click.argument("gz", type=click.Path(exists=True), required=True)
@click.option(
    "--max-emails",
    type=int,
    default=None,
    help="Stop after processing this many emails",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Perform a dry run without side effects",
    type=bool,
)
@click.option(
    "--fail-fast",
    is_flag=True,
    default=False,
    help="Stop on first error or push to dead-letter queue",
    type=bool,
)
@click.option(
    "--db-path",
    default=lambda: os.environ.get("DB_PATH", "~/.postmark/emails.db"),
    help="Path to the SQLite DB. Can also be set with DB_PATH env var.",
    type=str,
    required=True,
)
@click.option(
    "--parquet-dir",
    default=lambda: os.environ.get("PARQUET_DIR", "~/.postmark/parquet"),
    help="Directory to store Parquet files. Can also be set with PARQUET_DIR env var.",
    type=str,
    required=True,
)
@click.option(
    "--endpoint-url",
    default=lambda: os.environ.get("AWS_S3_ENDPOINT", "http://localhost:9000"),
    help="S3/MinIO endpoint URL; default from .env or http://localhost:9000",
    type=str,
)
@click.option(
    "--aws-access-key-id",
    default=lambda: os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    help="AWS/MinIO Access Key ID; default from .env or 'minioadmin'",
    type=str,
)
@click.option(
    "--aws-secret-access-key",
    default=lambda: os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    help="AWS/MinIO Secret Access Key; default from .env or 'minioadmin'",
    type=str,
)
@click.option(
    "--bucket-name",
    default=lambda: os.environ.get("S3_BUCKET", "postmark-attachments"),
    help="S3 bucket name; default from .env or 'postmark-attachments'",
    type=str,
)
@click.option(
    "--batch-size",
    type=int,
    default=lambda: int(os.environ.get("BATCH_SIZE", 1000)),
    show_default="BATCH_SIZE env or 1000",
    help="Number of emails to batch before writing to Parquet. Can also be set with BATCH_SIZE env var.",
)
def main(
    gz: str,
    db_path: str,
    parquet_dir: str,
    dry_run: bool = False,
    fail_fast: bool = False,
    endpoint_url: str = "http://localhost:9000",
    aws_access_key_id: str = "minioadmin",
    aws_secret_access_key: str = "minioadmin",
    bucket_name: str = "postmark-attachments",
    max_emails: Optional[int] = None,
    batch_size: int = 1000,
) -> None:
    load_dotenv()

    # Ensure ~/.postmark folder exists
    postmark_dir = Path("~/.postmark").expanduser()
    postmark_dir.mkdir(parents=True, exist_ok=True)

    # Normalize all paths
    gz = normalize_path(gz)
    db_path = normalize_path(db_path)
    parquet_dir = normalize_path(parquet_dir)
    parquet_path = Path(parquet_dir) / "emails.parquet"

    # Initialize the S3 client and SQLite
    s3_client = S3Client(
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        bucket_name=bucket_name,
    )
    conn = create_connection(db_path)
    create_schema(conn)

    # Initialize deduplication components
    bloom = BloomFilter(capacity=1000000, error_rate=0.001)
    recent_bodies: list[str] = []
    columnar_writer = ColumnarWriter(parquet_path, batch_size)

    # Read the specified gzip file in streaming mode
    count = 0
    with igz.IndexedGzipFile(gz) as f:
        for i, eml_raw in enumerate(extract_email_blobs(f), start=1):
            count = i
            if max_emails and count >= max_emails:
                click.echo(f"Reached max_emails limit of {max_emails}, stopping...")
                break
            err = process_email(
                eml_raw,
                i,
                bloom,
                recent_bodies,
                s3_client,
                conn,
                columnar_writer,
                dry_run,
                fail_fast,
                gz,
            )
            if err:
                click.echo(f"Error processing email {count}: {err}")
                continue
            if count % 1000 == 0:
                click.echo(f"Processed {count} emails so far...")

    if not dry_run:
        columnar_writer.flush()

    click.echo(f"Processed {count} lines total (dry run: {dry_run}).")


def normalize_path(p: str) -> str:
    return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(p))))
