**Context**
- Skeleton CLI runs.
- Time to persist data.

**Task**
Enhance `postmark-backfill`:

1. Move to indexed_gzip library so that we can seek in the file without having to decompress the whole thing
2. For each EML:
   - Parse → Email.
   - Dedup check (in‑memory Bloom + fuzzy).
   - Detect thread id.
3. Batch every 1000:
   - Insert SQLite rows (`emails`).
   - Append to Parquet via `columnar.write_parquet`.
4. Save attachments via `s3.put_bytes`.
5. Implement `--fail-fast` to stop on first error or push to dead‑letter queue.

**Tests**
- Fixture .gz with duplicates → only 2 stored.
- Assert attachments uploaded to MinIO with key `{uuid}/{mime_path}/filename`.

Write the code and the tests.
