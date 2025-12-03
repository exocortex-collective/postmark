**Context**
- The repo now contains scaffolding from Prompt 1.
- We need persistent metadata storage using SQLite.

**Task**
1. Create `postmark/storage/sqlite.py` with:
   - `create_connection(db_path: str) -> sqlite3.Connection`
   - `create_schema(conn)` that creates ONLY the `emails` table per the spec:

     ```
     CREATE TABLE emails(
       id INTEGER PRIMARY KEY,
       uuid TEXT UNIQUE NOT NULL,
       message_id TEXT,
       date INTEGER NOT NULL,
       subject TEXT,
       from_addr TEXT,
       schema_version INTEGER NOT NULL DEFAULT 1,
       parquet_file TEXT NOT NULL,
       parquet_row INTEGER NOT NULL
     );
     ```

2. Implement a very small migration system:
   - `schema_version` table with single row.
   - `upgrade(conn)` that applies the schema if fresh.

**Tests**
- `test_schema_created` connects to a tmp file, calls `upgrade`, asserts the `emails` table exists and `schema_version.version == 1`.

Write the code and the tests.
