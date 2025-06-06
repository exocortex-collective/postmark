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


def test_columnar_writer_partitions_by_month(tmp_path: Path) -> None:
    from datetime import datetime
    from postmark.email.parser import Email
    from postmark.storage.columnar import ColumnarWriter

    writer = ColumnarWriter(tmp_path)

    # Create sample emails spanning different months:
    emails = [
        Email(
            uuid="uuid-jan",
            message_id="<jan@domain>",
            date=datetime(2023, 1, 15),
            raw_msg="January message",
        ),
        Email(
            uuid="uuid-feb",
            message_id="<feb@domain>",
            date=datetime(2023, 2, 10),
            raw_msg="February message",
        ),
    ]

    for e in emails:
        writer.add(e)
    writer.flush()

    # Check that separate files exist for each month:
    jan_file = tmp_path / "2023-01.parquet"
    feb_file = tmp_path / "2023-02.parquet"
    assert jan_file.exists()
    assert feb_file.exists()

    # Optionally read back and verify data:
    import pyarrow as pa
    import pyarrow.parquet as pq
    jan_table = pq.read_table(str(jan_file))
    feb_table = pq.read_table(str(feb_file))
    assert jan_table.num_rows == 1
    assert feb_table.num_rows == 1
