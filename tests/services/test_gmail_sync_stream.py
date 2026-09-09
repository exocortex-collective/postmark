"""Tests for Pub/Sub stream resilience in gmail-sync.

Covers the two faults behind the 2026-09-07/08 crash loop: a streaming pull
error killing the process, and transient network errors bypassing the retry.
"""

import ssl
from typing import Any

import pytest

from postmark.reliability import BackoffConfig, GracefulShutdown
from postmark.services import gmail_sync
from postmark.services.gmail_sync import (
    RETRYABLE_NOTIFICATION_EXCEPTIONS,
    STREAM_BACKOFF_ATTEMPT_CAP,
    GmailSync,
)


class _FakeFuture:
    """Stands in for a StreamingPullFuture.

    `result()` raises whatever ended the stream, which is the behaviour that
    matters here: the real future re-raises the gRPC error on the calling thread.
    """

    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.cancelled = False

    def result(self) -> None:
        if self._error is not None:
            raise self._error

    def cancel(self) -> None:
        self.cancelled = True


class _SilentLogger:
    """Swallows log calls so a high-iteration test is not dominated by them."""

    def __getattr__(self, _name: str) -> Any:
        return lambda *args, **kwargs: None


class _FakeSubscriber:
    def __init__(self, futures: list[_FakeFuture]) -> None:
        self._futures = list(futures)
        self.subscribe_calls = 0

    def subscribe(self, *_args: Any, **_kwargs: Any) -> _FakeFuture:
        self.subscribe_calls += 1
        return self._futures.pop(0)


def _make_sync(futures: list[_FakeFuture]) -> GmailSync:
    """A GmailSync with only what _serve_subscription touches.

    Built without __init__ deliberately: the real constructor opens Postgres,
    Pub/Sub and Gmail clients, none of which this behaviour depends on.
    """
    sync = object.__new__(GmailSync)
    sync.subscriber = _FakeSubscriber(futures)  # type: ignore[assignment]
    sync.config = type(
        "_Config",
        (),
        {
            "subscription_path": "projects/p/subscriptions/s",
            "postgres": type("_Pg", (), {"pool_max_conn": 4})(),
        },
    )()
    sync._shutdown = GracefulShutdown()
    sync._stream_backoff_config = BackoffConfig(
        initial_delay=2.0, max_delay=300.0, multiplier=2.0, jitter=0.2, max_retries=0
    )
    sync._streaming_pull_future = None
    return sync


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Collect backoff delays instead of waiting them out."""
    delays: list[float] = []

    def _record(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(gmail_sync.time, "sleep", _record)
    return delays


def test_stream_error_reopens_the_subscription(no_sleep: list[float]) -> None:
    """A stream error must reconnect, not end the process.

    Before the fix the only handler was `except KeyboardInterrupt`, so this
    exception propagated out of run() and the container restarted.
    """
    error = RuntimeError("401 ACCESS_TOKEN_EXPIRED")
    futures = [_FakeFuture(error), _FakeFuture(error), _FakeFuture()]
    sync = _make_sync(futures)

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == 3  # type: ignore[attr-defined]
    assert [f.cancelled for f in futures] == [True, True, False]


def test_reconnect_backs_off_between_attempts(no_sleep: list[float]) -> None:
    """Repeated quick failures must wait longer each time, not reconnect hot."""
    error = RuntimeError("stream died")
    sync = _make_sync([_FakeFuture(error) for _ in range(4)] + [_FakeFuture()])

    sync._serve_subscription()

    assert len(no_sleep) == 4
    assert no_sleep == sorted(no_sleep), f"delays did not grow: {no_sleep}"
    assert no_sleep[0] < no_sleep[-1]


def test_long_lived_stream_resets_the_backoff(
    monkeypatch: pytest.MonkeyPatch, no_sleep: list[float]
) -> None:
    """A stream that ran a long time starts over at the shortest delay.

    Otherwise one blip a night inherits the previous night's backoff and the
    service waits minutes to reconnect after a single fault.
    """
    clock = iter([0.0, gmail_sync.STREAM_HEALTHY_SECONDS + 1.0] * 4)
    monkeypatch.setattr(gmail_sync.time, "monotonic", lambda: next(clock))

    error = RuntimeError("stream died")
    sync = _make_sync([_FakeFuture(error) for _ in range(3)] + [_FakeFuture()])

    sync._serve_subscription()

    # Every failure followed a healthy stream, so every delay is the first-attempt
    # delay rather than an escalating one.
    assert max(no_sleep) < 4.0, f"backoff escalated across healthy streams: {no_sleep}"


def test_reconnecting_past_the_overflow_point(
    monkeypatch: pytest.MonkeyPatch, no_sleep: list[float]
) -> None:
    """The attempt counter must not grow until the delay maths overflows.

    calculate_backoff_delay computes multiplier ** attempt before clamping to
    max_delay, and 2.0 ** 1024 raises OverflowError. This loop has no retry
    limit and the incident that prompted it reached 1119 consecutive failures,
    so an uncapped counter would crash inside the handler meant to prevent the
    crash.
    """
    # Each failure logs a formatted traceback, which dominates the runtime at
    # this iteration count and tests nothing here.
    monkeypatch.setattr(gmail_sync, "logger", _SilentLogger())

    error = RuntimeError("stream died")
    failures = 1100
    sync = _make_sync([_FakeFuture(error) for _ in range(failures)] + [_FakeFuture()])

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == failures + 1  # type: ignore[attr-defined]
    assert len(no_sleep) == failures
    assert max(no_sleep) <= 300.0 * 1.2  # max_delay plus its jitter


def test_shutdown_stops_the_reconnect_loop(no_sleep: list[float]) -> None:
    """Once shutdown is requested the loop must not resubscribe."""
    sync = _make_sync([_FakeFuture(RuntimeError("stream died"))])
    sync._shutdown.initiate()

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == 0  # type: ignore[attr-defined]


def test_attempt_cap_is_below_the_overflow_threshold() -> None:
    """The cap only exists to keep multiplier ** attempt finite."""
    assert STREAM_BACKOFF_ATTEMPT_CAP < 1024
    assert 2.0**STREAM_BACKOFF_ATTEMPT_CAP < float("inf")


@pytest.mark.parametrize(
    "error",
    [
        ssl.SSLError("UNEXPECTED_EOF_WHILE_READING"),
        BrokenPipeError(32, "Broken pipe"),
        ConnectionResetError(54, "Connection reset by peer"),
        TimeoutError("timed out"),
    ],
)
def test_transient_network_errors_are_retryable(error: Exception) -> None:
    """These reached the nack path without a single retry.

    ssl.SSLError and BrokenPipeError descend from OSError without passing
    through urllib.error.URLError or TimeoutError, so the old tuple matched
    neither and retry_with_backoff re-raised on the first attempt while the log
    claimed the retries had been exhausted.
    """
    assert isinstance(error, RETRYABLE_NOTIFICATION_EXCEPTIONS)


def test_stream_diagnostics_never_raise() -> None:
    """Diagnostics run on the error path and must not mask the real fault."""
    sync = _make_sync([])
    # No transport on the fake subscriber: the unhappy path for this helper.
    facts = sync._stream_failure_diagnostics()

    assert "local_utc" in facts
    assert facts["token_expiry_utc"] == "unavailable"
