"""Every UPDATE that returns a row to 'pending' must strip claimed_by with it.

A stale reset that clears claimed_at but leaves the token behind hands the next
claimant a row still certified to the previous one. The worker that stalled then
calls complete() with its old token, the token matches, and it retires a job
another worker is mid-flight on -- worse than carrying no token at all, because
the token affirmatively vouches for the wrong claimant.

Reproduced against a live Postgres before this was fixed: complete() returned
True and the row went 'completed' while the second worker still held it.

These are source assertions rather than live ones because postmark has no
Postgres test layer; the invariant is cheap to state and expensive to rediscover.
"""

import re
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "postmark" / "services"

# Any UPDATE of queue whose SET list makes the row claimable again. Stops at
# WHERE so a later clause mentioning claimed_by cannot satisfy the assertion.
#
# `.*?` and not `[^)]*?`: a SET list is allowed to contain parentheses, and the
# shape that matters does -- `next_attempt_at = clock_timestamp() + (INTERVAL
# '1 second' * %s)`, which is what a release or a backoff-retry looks like.
# Against `[^)]*?` such a statement does not match AT ALL, so it would be
# skipped rather than checked, and the assertion below would still pass on the
# other, already-guarded site.
TO_PENDING = re.compile(r"UPDATE queue\s+SET status = 'pending'.*?(?=WHERE)", re.S)

# Pinned per file, so a newly added pending-UPDATE has to declare itself here
# rather than being quietly uncovered.
EXPECTED = {"parse_worker.py": 1, "attachment_worker.py": 1}


@pytest.mark.parametrize("name,count", sorted(EXPECTED.items()))
def test_returning_a_row_to_pending_also_clears_the_claim_token(
    name: str, count: int
) -> None:
    matches = TO_PENDING.findall((SRC / name).read_text())
    assert len(matches) == count, (
        f"expected {count} queue->pending UPDATE(s) in {name}, found "
        f"{len(matches)} -- a new one needs the same guard, or one has moved"
    )
    for sql in matches:
        assert "claimed_by = NULL" in sql, (
            f"{name} returns a row to pending without clearing claimed_by:\n{sql}\n"
            "The next claimant inherits the previous worker's token."
        )
