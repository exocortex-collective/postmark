from pathlib import Path

import pyarrow as pa

from postmark.storage.columnar import read_parquet, write_parquet


def test_write_and_read_parquet(tmp_path: Path) -> None:
    data = {
        "uuid": [f"uuid-{i}" for i in range(10)],
        "body_text": [f"body {i}" for i in range(10)],
    }
    table = pa.table(data)
    out_path = tmp_path / "test.parquet"

    write_parquet(table, out_path)

    read_back = read_parquet(out_path)
    assert read_back.num_rows == 10
    assert read_back.num_columns == 2
