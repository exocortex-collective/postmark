**Context**
- SQLite layer is in place.
- Need columnar storage for email bodies.

**Task**
1. Create `postmark/storage/columnar.py` using `pyarrow`:
   - `write_parquet(df: pa.Table, path: Path) -> None`
     * Write with ZSTD compression.
     * Create parent dirs.
2. Add `read_parquet(path) -> pa.Table`.

**Tests**
- Build a 10‑row PyArrow Table with columns `uuid` and `body_text`.
- Write to a tmp dir, read back, assert row count == 10.

Write the code and the tests.
