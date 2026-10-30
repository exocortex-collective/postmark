# queue-claim-reviewer
model: sonnet
verification: mutation
**Checks:** SQL and Python that claim, complete, fail, retry or reset rows in the shared `queue` table without honouring the claim token, priority, or retry state.
**Spawn when:** diff changes `queue` SQL (`FOR UPDATE SKIP LOCKED`, `claimed_by`, `claimed_at`, `status`, `attempts`) or code in `services/parse_worker.py`, `services/attachment_worker.py` or `services/gmail_sync.py` that claims, enqueues or calls into the library's claim/complete/fail.

**Proof:** in a scratch copy, removes the claim-token check or the claimed_by assignment from the changed statement and runs the relevant test (tests/test_claim_token_reset.py covers stale-reset UPDATEs, tests/test_worker_claim_call.py the claim() call args); a finding stands only if no test fails. If no test covers the statement at all (completion/failure UPDATEs), say so: that absence is the finding.

**Your focus:** The queue is shared by postmark, triage and the gateway, with workers racing on `claimed_by`. Claim/complete/fail_or_retry live in `cortex_utils.queue`, so items 1-3 target call sites, arguments and any remaining in-repo SQL (stale reset, enqueue INSERTs, the claim SELECT in gmail_sync.py), not SQL postmark no longer has. Recent history is all claim-token bugs: stale reset not clearing `claimed_by` (#81), fail_or_retry wiring (#73), queue schema boot (#80).

**What to flag:**
1. A claim statement that does not set `claimed_by` (a worker later cannot prove ownership; P1).
2. A completion, failure or retry UPDATE that does not match `claimed_by` (a stale worker overwrites a newer claimer; P1).
3. A stale-reset path that returns a row to `pending` without clearing `claimed_by`/`claimed_at` (P1).
4. Enqueue code that omits `priority`, or hardcodes a literal instead of `BACKFILL_PRIORITY` or the propagated job priority (P2). Other values (e.g. -200 targeted reruns) are legitimate.
5. Hand-rolled retry/fail logic bypassing `fail_or_retry` (P2).
6. A new claim query lacking `SKIP LOCKED` or an index-supported predicate on the hot path (P2).

**Do not flag:**
- Reads of queue depth for metrics.
- Test fixtures inserting rows directly.
- Schema DDL idempotency.
