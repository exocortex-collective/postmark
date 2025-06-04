import indexed_gzip as igz
import os
import tempfile
import sys
from pprint import pprint
from typing import IO, Iterator, Optional

import click
import pyarrow as pa

from postmark.email.dedup import BloomFilter, should_store
from postmark.email.parser import Email, parse_eml
from postmark.email.threading import detect_thread
from postmark.storage.s3 import S3Client
from postmark.storage.sqlite import create_connection, create_schema, insert_email, write_dead_letter
from postmark.storage.columnar import write_parquet


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
    current_email = []
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


@click.command()
@click.argument("gz", type=click.Path(exists=True))
@click.option(
    "--db-path", default="~/.postmark/emails.db", help="Path to the SQLite DB"
)
@click.option(
    "--parquet-dir",
    default="~/.postmark/parquet",
    help="Directory to store Parquet files",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Perform a dry run without side effects",
)
@click.option(
    "--fail-fast",
    is_flag=True,
    default=False,
    help="Stop on first error or push to dead-letter queue",
)
def main(gz: str, db_path: str, parquet_dir: str, dry_run: bool, fail_fast: bool = False) -> None:
    # Normalize all paths
    gz = normalize_path(gz)
    db_path = normalize_path(db_path)
    parquet_dir = normalize_path(parquet_dir)

    # Initialize the S3 client and SQLite
    s3_client = S3Client(
        endpoint_url=None,
        aws_access_key_id="CHANGEME",
        aws_secret_access_key="CHANGEME",
        bucket_name="postmark-attachments",
    )
    conn = create_connection(db_path)
    create_schema(conn)

    # Initialize deduplication components
    bloom = BloomFilter(capacity=1000000, error_rate=0.001)
    recent_bodies: list[str] = []
    batch_rows = []
    batch_size = 1000

    # Read the specified gzip file in streaming mode
    count = 0
    with igz.IndexedGzipFile(gz) as f:
        for i, eml_raw in enumerate(_extract_eml_blobs(f), start=1):
            count = i
            try:
                # Always parse the EML to perform all checks
                parse_eml_result = parse_eml_from_bytes(eml_raw)

                # Dedup check
                if not should_store(parse_eml_result, bloom, recent_bodies):
                    continue

                # Thread detection
                thread_id = detect_thread(parse_eml_result, resolver=_fake_resolver)

                # If dry_run, skip side effects, but keep logic checks
                if dry_run:
                    # No storage side effects
                    pass
                else:
                    # Save attachments
                    if parse_eml_result.attachments:
                        for attach in parse_eml_result.attachments:
                            key = f"{parse_eml_result.uuid}/1.2/{attach}"
                            s3_client.put_bytes(key, b"PLACEHOLDER_ATTACHMENT_CONTENT")

                    # Insert into SQLite
                    insert_email(conn, parse_eml_result, thread_id)

                    # Add to batch for Parquet write
                    batch_rows.append(
                        {
                            "uuid": parse_eml_result.uuid,
                            "subject": parse_eml_result.subject,
                            "from_addr": parse_eml_result.from_addr,
                            "body_text": parse_eml_result.body_text,
                        }
                    )

                    # Batch Parquet write
                    if len(batch_rows) >= batch_size:
                        write_parquet(pa.Table.from_pylist(batch_rows), os.path.join(parquet_dir, "emails.parquet"))
                        batch_rows.clear()

                if count % 1000 == 0:
                    click.echo(f"Processed {count} emails so far...")

            except Exception as e:
                if fail_fast:
                    raise
                else:
                    write_dead_letter(conn, gz, i, e)
                click.echo(f"Error processing email {count}: {e}")
                continue

    # Write any remaining batch
    if not dry_run and batch_rows:
        write_parquet(pa.Table.from_pylist(batch_rows), os.path.join(parquet_dir, "emails.parquet"))
        batch_rows.clear()

    click.echo(f"Processed {count} lines total (dry run: {dry_run}).")


