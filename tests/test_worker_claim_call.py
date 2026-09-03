"""The ported workers call the queue library with the right arguments.

Mocked, because postmark has no Postgres test layer and these are the arguments
rather than the SQL -- cortex-utils tests what claim() does; this tests that we
ask it for the right thing.

It exists because the port left the suite blind. Four separate regressions, each
a production outage or a silent correctness loss, all passed 203 green:

    claim(..., "parse") -> "attachment"        203 passed
    worker=self._worker -> worker="anonymous"  203 passed
    visibility_timeout_min= dropped            203 passed
    complete(..., self._worker) -> "anonymous" 203 passed

And the file that used to keep this code honest, test_claim_token_reset, is now
vacuous for these two modules by design: with the raw SQL gone its match list is
empty, so the loop asserting `claimed_by = NULL` never runs. It still guards
against a NEW raw pending-UPDATE appearing, which is worth keeping -- but it
stopped being a check on the claim path the moment the port landed.
"""

import importlib
from typing import Any
from unittest.mock import patch

import pytest

CASES = [
    ("postmark.services.parse_worker", "ParseWorker", "parse", "parse-worker"),
    (
        "postmark.services.attachment_worker",
        "AttachmentWorker",
        "attachment",
        "attachment-worker",
    ),
]


def _worker(module: str, cls: str) -> Any:
    """A worker instance without running __init__, which wants real config."""
    mod = importlib.import_module(module)
    obj = object.__new__(getattr(mod, cls))
    obj._worker = "tok@host:1"
    obj._get_pg_conn = lambda: "CONN"  # type: ignore[method-assign]
    return mod, obj


@pytest.mark.parametrize(("module", "cls", "queue", "service"), CASES)
def test_claim_asks_for_the_right_queue_token_and_timeout(
    module: str, cls: str, queue: str, service: str
) -> None:
    mod, obj = _worker(module, cls)
    with patch.object(mod, "claim", return_value=[]) as claimed:
        obj._claim_jobs(7)

    args, kwargs = claimed.call_args
    assert args[1] == queue, (
        f"{cls} claimed {args[1]!r} -- a worker draining the wrong queue is "
        "silent, and both queues keep growing"
    )
    assert kwargs["worker"] == "tok@host:1", "the claim token must be this process's"
    assert kwargs["limit"] == 7
    assert kwargs["visibility_timeout_min"] == mod.VISIBILITY_TIMEOUT_MINUTES, (
        "dropping this falls back to the library's 30 minutes, so a crashed "
        "worker's jobs sit six times longer before anyone else can have them"
    )


@pytest.mark.parametrize(("module", "cls", "queue", "service"), CASES)
def test_reports_carry_the_claim_token(
    module: str, cls: str, queue: str, service: str
) -> None:
    """complete() and fail_or_retry() match on the token. Passing anything else
    means every report bounces and every job is done twice.
    """
    mod, obj = _worker(module, cls)
    with patch.object(mod, "complete", return_value=True) as completed:
        obj._complete_job(42)
    assert completed.call_args[0][1:] == (42, "tok@host:1")

    conn = type("C", (), {"rollback": lambda self: None})()
    obj._get_pg_conn = lambda: conn  # type: ignore[method-assign]
    with patch.object(mod, "fail_or_retry", return_value="pending") as failed:
        obj._fail_job(42, "boom")
    assert failed.call_args[0][1:] == (42, "boom", "tok@host:1")


@pytest.mark.parametrize(("module", "cls", "queue", "service"), CASES)
def test_a_bounced_completion_is_not_counted_as_success(
    module: str, cls: str, queue: str, service: str
) -> None:
    """complete() returns False when the claim is gone. Discarding that let the
    worker increment its success counter for a job the queue still shows
    unfinished and another worker is redoing -- a metric that lies in exactly
    the situation you would be reading it.
    """
    mod, obj = _worker(module, cls)
    with (
        patch.object(mod, "complete", return_value=False),
        patch.object(mod, "ERRORS") as errors,
    ):
        obj._complete_job(42)
    errors.labels.assert_called_once_with(service=service, error_type="claim_lost")
