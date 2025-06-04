import os
import tempfile
from pathlib import Path
from sqlite3 import Connection
from typing import IO, Any, Iterator, Optional

import click
import indexed_gzip as igz
import pyarrow as pa
from dotenv import load_dotenv

from postmark.email.dedup import BloomFilter, should_store
from postmark.email.parser import Email, parse_eml
from postmark.email.threading import detect_thread
from postmark.storage.columnar import write_parquet
from postmark.storage.s3 import S3Client
from postmark.storage.sqlite import (
    create_connection,
    create_schema,
    insert_email,
    write_dead_letter,
)


def normalize_path(p: str) -> str:
    return os.path.abspath(os.path.normpath(os.path.expandvars(os.path.expanduser(p))))


def _fake_resolver(key: str) -> Optional[Email]:
    """Fake resolver for thread detection during dry run."""
    return None


def parse_eml_from_bytes(eml_raw: bytes) -> Email:
    """
    Write the bytes to a temporary file and parse it with parser.parse_eml.
    """
    from pathlib import Path

    with tempfile.NamedTemporaryFile(suffix=".eml", delete=False) as tf:
        tf.write(eml_raw)
        tf.flush()
        tmp_path = Path(tf.name)
    parsed = parse_eml(tmp_path)
    tmp_path.unlink(missing_ok=True)
    return parsed


def _extract_eml_blobs(f: IO[bytes]) -> Iterator[bytes]:
    """
    Extract complete EML blobs from a mbox file, looking for lines starting with 'From '.
    """
    current_email: list[bytes] = []
    for line in f:
        # Check if this line indicates a new email boundary
        if line.startswith(b"From "):
            # If we already accumulated lines, yield them
            if current_email:
                yield b"".join(current_email)
                current_email.clear()
        current_email.append(line)

    # Yield any remaining lines after the final boundary
    if current_email:
        yield b"".join(current_email)


def process_email(
    eml_raw: bytes,
    i: int,
    bloom: BloomFilter,
    recent_bodies: list[str],
    s3_client: S3Client,
    conn: Connection,
    parquet_dir: str,
    batch_rows: list[dict[str, Any]],
    dry_run: bool,
    fail_fast: bool,
    gz: str,
    batch_size: int,
) -> Optional[Exception]:
    try:
        parse_eml_result = parse_eml_from_bytes(eml_raw)
        if not should_store(parse_eml_result, bloom, recent_bodies):
            return None
        thread_id = detect_thread(parse_eml_result, resolver=_fake_resolver)
        if dry_run:
            pass
        else:
            if parse_eml_result.attachments:
                for attach in parse_eml_result.attachments:
                    key = f"{parse_eml_result.uuid}/1.2/{attach}"
                    s3_client.put_bytes(key, b"PLACEHOLDER_ATTACHMENT_CONTENT")
            insert_email(conn, parse_eml_result, thread_id)
            batch_rows.append(
                {
                    "uuid": parse_eml_result.uuid,
                    "subject": parse_eml_result.subject,
                    "from_addr": parse_eml_result.from_addr,
                    "body_text": parse_eml_result.body_text,
                }
            )
            if len(batch_rows) >= batch_size:
                out_path = Path(parquet_dir) / "emails.parquet"
                write_parquet(
                    pa.Table.from_pylist(batch_rows),
                    out_path,
                )
                batch_rows.clear()
        return None
    except Exception as e:
        if fail_fast:
            raise
        else:
            write_dead_letter(conn, gz, i, e)
        return e


@click.command()  # type: ignore[misc]
@click.argument("gz", type=click.Path(exists=True), required=True)  # type: ignore[misc]
@click.option(
    "--db-path",
    default="~/.postmark/emails.db",
    help="Path to the SQLite DB",
    type=str,
    required=True,
)  # type: ignore[misc]
@click.option(
    "--parquet-dir",
    default="~/.postmark/parquet",
    help="Directory to store Parquet files",
    type=str,
    required=True,
)  # type: ignore[misc]
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Perform a dry run without side effects",
    type=bool,
)  # type: ignore[misc]
@click.option(
    "--fail-fast",
    is_flag=True,
    default=False,
    help="Stop on first error or push to dead-letter queue",
    type=bool,
)  # type: ignore[misc]
@click.option(
    "--endpoint-url",
    default=lambda: os.environ.get("AWS_S3_ENDPOINT", "http://localhost:9000"),
    help="S3/MinIO endpoint URL; default from .env or http://localhost:9000",
    type=str,
)  # type: ignore[misc]
@click.option(
    "--aws-access-key-id",
    default=lambda: os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    help="AWS/MinIO Access Key ID; default from .env or 'minioadmin'",
    type=str,
)  # type: ignore[misc]
@click.option(
    "--aws-secret-access-key",
    default=lambda: os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    help="AWS/MinIO Secret Access Key; default from .env or 'minioadmin'",
    type=str,
)  # type: ignore[misc]
@click.option(
    "--bucket-name",
    default=lambda: os.environ.get("S3_BUCKET", "postmark-attachments"),
    help="S3 bucket name; default from .env or 'postmark-attachments'",
    type=str,
)  # type: ignore[misc]
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
) -> None:
    load_dotenv()

    # Ensure ~/.postmark folder exists
    postmark_dir = Path("~/.postmark").expanduser()
    postmark_dir.mkdir(parents=True, exist_ok=True)

    # Normalize all paths
    gz = normalize_path(gz)
    db_path = normalize_path(db_path)
    parquet_dir = normalize_path(parquet_dir)

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
    batch_rows: list[dict[str, Any]] = []
    batch_size = 1000

    # Read the specified gzip file in streaming mode
    count = 0
    with igz.IndexedGzipFile(gz) as f:
        for i, eml_raw in enumerate(_extract_eml_blobs(f), start=1):
            count = i
            err = process_email(
                eml_raw,
                i,
                bloom,
                recent_bodies,
                s3_client,
                conn,
                parquet_dir,
                batch_rows,
                dry_run,
                fail_fast,
                gz,
                batch_size,
            )
            if err:
                click.echo(f"Error processing email {count}: {err}")
                continue
            if count % 1000 == 0:
                click.echo(f"Processed {count} emails so far...")

    # Write any remaining batch
    if not dry_run and batch_rows:
        out_path = Path(parquet_dir) / "emails.parquet"
        write_parquet(
            pa.Table.from_pylist(batch_rows),
            out_path,
        )
        batch_rows.clear()

    click.echo(f"Processed {count} lines total (dry run: {dry_run}).")
