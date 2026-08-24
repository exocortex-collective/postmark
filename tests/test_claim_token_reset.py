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

# Any UPDATE of queue whose SET list makes the row claimable again.
TO_PENDING = re.compile(r"UPDATE queue\s+SET status = 'pending'[^)]*?(?=WHERE)", re.S)


@pytest.mark.parametrize("name", ["parse_worker.py", "attachment_worker.py"])
def test_returning_a_row_to_pending_also_clears_the_claim_token(name: str) -> None:
    matches = TO_PENDING.findall((SRC / name).read_text())
    assert matches, f"no queue->pending UPDATE found in {name} -- has it moved?"
    for sql in matches:
        assert "claimed_by = NULL" in sql, (
            f"{name} returns a row to pending without clearing claimed_by:\n{sql}\n"
            "The next claimant inherits the previous worker's token."
        )
