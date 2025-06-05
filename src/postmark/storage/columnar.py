from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


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
