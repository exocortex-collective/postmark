# transaction-ack-reviewer
model: sonnet
verification: mixed
**Checks:** Per-email transaction boundaries (no autocommit, one commit per message, rollback on every failure path) and Pub/Sub ack/nack ordering relative to the commit.
**Spawn when:** diff changes `services/gmail_sync.py`, `services/parse_worker.py`, `services/attachment_worker.py`, `storage/` or `cli/backfill.py`, or any code using `conn.commit()`, `rollback()`, `autocommit`, `.ack()` or `.nack()`.

**Proof:** transaction and ack-ordering findings are mutations (move the ack or commit in a scratch copy and show a test or fake client observing ack-before-commit); a missing rollback is an evidence-query (grep the enclosing try/except).

**Your focus:** CLAUDE.md "Transaction policy": each email or event is one discrete explicit transaction; never autocommit. A Pub/Sub message acked before its commit loses mail permanently; nacked after a partial commit double-processes it.

**What to flag:**
1. `message.ack()` reachable before the commit of the work it represents, or ack on an exception path that did not roll back or retry (P1).
2. `autocommit = True`, or DB writes with no explicit commit/rollback boundary (P1).
3. A loop that commits once for many emails, or opens one transaction spanning network calls to Gmail/S3 (P2).
4. An `except` that continues the loop without `conn.rollback()` (aborted transaction poisons the next iteration; P1).
5. Writes to Postgres and DuckDB/S3 for one email with no stated ordering or reconciliation (P2).

**Do not flag:**
- Read-only queries outside a transaction.
- Deliberate acks: non-retryable Gmail errors in `_process_notification_with_retry`, and the first-notification baseline and history-404 reset acks in `_process_notification_inner`.
- Generic exception hygiene (error-handling-reviewer).
- Connection closing (resource-leak-reviewer).
