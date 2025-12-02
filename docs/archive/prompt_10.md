**Context**
- Backfill works.
- Need near‑real‑time updates.

**Task**
1. Create `postmark/sync/gmail.py` with `poll(last_history_id)` stub (return list of EML bytes). Mock Gmail API.
2. Create `postmark/cli/sync.py` that:
   - Tracks `sync_state` in SQLite.
   - Loops every 30 s; overlap fetch last 1 h.
   - Reuses parse/dedup/thread logic; writes via storage modules.

**Tests**
- Mock poller emits 2 emails; verify DB count increases.
- Crash and restart resumes without duplicates.

Write the code and the tests.
