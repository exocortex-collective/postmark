import re
import tempfile
from pprint import pprint
from sqlite3 import Connection
from typing import IO, Iterator, Optional

from postmark.archive.storage.columnar import ColumnarWriter
from postmark.archive.threading import detect_thread
from postmark.email.dedup import BloomFilter, should_store
from postmark.email.parser import Email, parse_email
from postmark.storage.s3 import S3Client
from postmark.storage.sqlite import write_attachment, write_dead_letter, write_email


def sanitize_s3_key(key: str) -> str:
    key = re.sub(r"(^[./]+|[./]+$)", "", key)
    key = key.replace("..", "").replace("./", "").replace("/.", "")
    return key


def process_email(
    eml_raw: bytes,
    i: int,
    bloom: BloomFilter,
    recent_bodies: list[str],
    s3_client: S3Client,
    conn: Connection,
    columnar_writer: ColumnarWriter,
    dry_run: bool,
    fail_fast: bool,
    gz: str,
) -> Optional[Exception]:
    parse_email_result = None
    try:
        parse_email_result = parse_email_from_bytes(eml_raw)
        if not should_store(parse_email_result, bloom, recent_bodies):
            return None
        _ = detect_thread(parse_email_result, resolver=_fake_resolver)
        if dry_run:
            pass
        else:
            email_id = write_email(conn, parse_email_result)
            if parse_email_result.attachments:
                print(f"Processing {len(parse_email_result.attachments)} attachments")
                for idx, attach in enumerate(parse_email_result.attachments):
                    name = attach.name
                    mime_type = attach.mime_type
                    payload = attach.payload
                    size = attach.size
                    key = f"{parse_email_result.uuid}/{idx+1}/{name}"
                    safe_key = sanitize_s3_key(key)
                    s3_client.put_bytes(safe_key, payload)
                    write_attachment(
                        conn,
                        email_id,
                        "s3",
                        safe_key,
                        idx,
                        name,
                        mime_type,
                        size,
                    )
            columnar_writer.add(parse_email_result)
        return None
    except Exception as e:
        if fail_fast:
            pprint(parse_email_result)
            raise
        else:
            write_dead_letter(conn, gz, i, e)
        return e


def _fake_resolver(key: str) -> Optional[Email]:
    """Fake resolver for thread detection during dry run."""
    return None


def parse_email_from_bytes(email_raw: bytes) -> Email:
    """
    Write the bytes to a temporary file and parse it with parser.parse_email.
    """
    from pathlib import Path

    with tempfile.NamedTemporaryFile(suffix=".eml", delete=False) as tf:
        tf.write(email_raw)
        tf.flush()
        tmp_path = Path(tf.name)
    parsed = parse_email(tmp_path)
    tmp_path.unlink(missing_ok=True)
    return parsed


def extract_email_blobs(f: IO[bytes]) -> Iterator[bytes]:
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


# TODO:
# From blank
# rename addr to address
# None is in in_reply_to
# Some dates are 0
# unknown encoding: text/plain
# unknown encoding: "utf-8" format=flowed
# unknown encoding: charset=us-ascii
# unknown encoding: _iso-2022-jp$esc
# split parquet files
