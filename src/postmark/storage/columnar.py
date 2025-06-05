from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from postmark.email.parser import Email


def write_parquet(df: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # ADD THIS CHECK:
    if path.exists():
        old_table = pq.read_table(str(path))
        merged = pa.concat_tables([old_table, df])
        pq.write_table(merged, str(path), compression="zstd")
    else:
        pq.write_table(df, str(path), compression="zstd")


def read_parquet(path: Path) -> pa.Table:
    return pq.read_table(str(path))


def create_email_record(email: Email) -> dict[str, Any]:
    return {
        "uuid": email.uuid,
        "msg_id": email.message_id,
        "date": pa.scalar(email.date, type=pa.timestamp("s")),
        "raw_msg": email.raw_msg,
    }


def create_email_table(batch: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [
            ("uuid", pa.string()),
            ("msg_id", pa.string()),
            ("date", pa.timestamp("s")),
            ("raw_msg", pa.string()),
        ]
    )
    return pa.Table.from_pylist(batch, schema)


class ColumnarWriter:
    def __init__(self, path: Path, batch_size: int = 1000) -> None:
        self.path = path
        self.batch_size = batch_size
        self.batch: list[dict[str, Any]] = []

    def add(self, email: Email) -> None:
        self.batch.append(create_email_record(email))
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.batch:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        table = create_email_table(self.batch)
        if self.path.exists():
            old_table = pq.read_table(str(self.path))
            merged = pa.concat_tables([old_table, table])
            pq.write_table(merged, str(self.path), compression="zstd")
        else:
            pq.write_table(table, str(self.path), compression="zstd")
        self.batch.clear()
