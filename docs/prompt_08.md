**Context**
- Core libraries done.
- We now ingest Gmail exports.

**Task**
1. Create package `postmark/cli/backfill.py` using `click`.
2. Command `postmark-backfill [TGZ] --db-path --parquet-dir --dry-run`.
3. Inside, open the .tgz (use `tarfile`), iterate `.eml` entries, but **for now** just count them and print progress every 1000.

**Tests**
- Tiny test .tgz with 3 EMLs; `--dry-run` prints “Processed 3”.

Write the code and the tests.
