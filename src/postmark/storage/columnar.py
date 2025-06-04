from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(df: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df, str(path), compression="zstd")


def read_parquet(path: Path) -> pa.Table:
    return pq.read_table(str(path))
