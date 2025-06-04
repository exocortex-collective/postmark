**Context**
- Core libraries done.
- We now ingest Gmail exports.

**Task**
1. Create package `postmark/cli/backfill.py` using `click`.
2. Command `postmark-backfill [GZ] --db-path --parquet-dir --dry-run`.  [GZ] is `~/Downloads/All\ mail\ Including\ Spam\ and\ Trash-003.gz`
3. Inside, open the .gz (use `gzip`), iterate entries, but **for now** just count them and print progress every 1000.

**Tests**
- Tiny test .gz with 3 EMLs; `--dry-run` prints “Processed 3”.

Write the code and the tests.
