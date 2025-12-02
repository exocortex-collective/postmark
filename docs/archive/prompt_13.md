**Context**
- Core pipeline feature-complete.
- Finish auxiliary tools.

**Task**
1. `postmark/cli/reindex.py` scans all emails, recomputes `normalized_hash`, updates rows.
2. `postmark/cli/add_test.py`:
   - Accepts path to problematic EML.
   - Runs progressive anonymisation until original exception reproduced.
   - Saves minimal case under `tests/data/edge/`.

**Tests**
- Corrupt EML triggers anonymiser; resulting file is smaller yet causes same error when re‑parsed.

Write the code and the tests.
