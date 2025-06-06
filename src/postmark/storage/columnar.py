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
        self.root_path = path  # Directory for partitioned data
        self.batch_size = batch_size  # Flush threshold per month partition
        from collections import defaultdict

        self.month_batches: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def add(self, email: Email) -> None:
        record = create_email_record(email)
        dt = record["date"].as_py()  # pa.scalar -> Python datetime
        month_key = dt.strftime("%Y-%m")
        self.month_batches[month_key].append(record)
        if len(self.month_batches[month_key]) >= self.batch_size:
            self._flush_month_partition(month_key)

    def _flush_month_partition(self, month_key: str) -> None:
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        records = self.month_batches[month_key]
        if not records:
            return

        out_file = self.root_path / f"{month_key}.parquet"
        out_file.parent.mkdir(parents=True, exist_ok=True)

        new_table = create_email_table(records)

        # Merge with existing file if present
        if out_file.exists():
            old_table = pq.read_table(str(out_file))
            new_table = pa.concat_tables([old_table, new_table])

        # Sort by date and write
        sort_indices = pc.sort_indices(new_table, sort_keys=[("date", "ascending")])
        sorted_table = pc.take(new_table, sort_indices)
        pq.write_table(sorted_table, str(out_file), compression="zstd")

        self.month_batches[month_key].clear()

    def flush(self) -> None:
        for month_key in list(self.month_batches.keys()):
            self._flush_month_partition(month_key)
