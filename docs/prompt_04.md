**Context**
- Parquet helper ready.
- We need binary attachment storage using S3 API.

**Task**
1. Assume devs run `docker‑compose` with MinIO at `localhost:9000`, creds `minioadmin:minioadmin`.
2. Create `postmark/storage/s3.py` with:
   - `S3Client` wrapper (`put_bytes`, `get_bytes`, `list_keys(prefix)`).
3. Allow bucket auto‑create if missing.

**Tests**
- Spin up MinIO using `moto` or mock‑s3.
- `test_put_and_get` uploads a small PDF, fetches it back, asserts equality.

Write the code and the tests.
