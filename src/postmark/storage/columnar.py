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
        "date": pa.scalar(email.date, type=pa.timestamp("ms")),
        "raw_msg": email.raw_msg,
    }


def create_email_table(batch: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [
            ("uuid", pa.string()),
            ("msg_id", pa.string()),
            ("date", pa.timestamp("ms")),
            ("raw_msg", pa.string()),
        ]
    )
    return pa.Table.from_pylist(batch, schema)


class ColumnarWriter:
    def __init__(self, path: Path, batch_size: int = 1000) -> None:
        # Treat the given path as the root directory for monthly partitions
        self.root_path = path
        self.batch_size = batch_size
        self.batch: list[dict[str, Any]] = []

    def add(self, email: Email) -> None:
        self.batch.append(create_email_record(email))
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.batch:
            return

        from collections import defaultdict

        grouped_records = defaultdict(list)
        for record in self.batch:
            # Assuming record["date"] is a pa.scalar
            dt = record["date"].as_py()
            partition_name = dt.strftime("%Y-%m")
            grouped_records[partition_name].append(record)

        for partition_name, records in grouped_records.items():
            out_file = self.root_path / f"{partition_name}.parquet"
            out_file.parent.mkdir(parents=True, exist_ok=True)
            table = create_email_table(records)
            if out_file.exists():
                old_table = pq.read_table(str(out_file))
                merged = pa.concat_tables([old_table, table])
                pq.write_table(merged, str(out_file), compression="zstd")
            else:
                pq.write_table(table, str(out_file), compression="zstd")

        self.batch.clear()
