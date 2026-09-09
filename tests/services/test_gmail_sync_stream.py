"""Tests for Pub/Sub stream resilience in gmail-sync.

Covers the streaming-pull error that killed the process during the 2026-09-07/08
crash loop, and the transient network errors that bypassed the notification retry
(observed 2026-09-09).
"""

import itertools
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

import pytest

from postmark.reliability import BackoffConfig, GracefulShutdown
from postmark.services import gmail_sync
from postmark.services.gmail_sync import (
    STREAM_BACKOFF_ATTEMPT_CAP,
    STREAM_BACKOFF_CONFIG,
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


class _StoppingFuture(_FakeFuture):
    """A stream closing because shutdown cancelled it.

    The real future returns from `result()` once the manager closes cleanly, and
    that only happens after something asked it to stop. Modelling the request is
    what makes the loop terminate the way it does in production, rather than
    relying on the fake running out of futures.
    """

    def __init__(self, shutdown: GracefulShutdown) -> None:
        super().__init__()
        self._shutdown = shutdown

    def result(self) -> None:
        self._shutdown.initiate()


class _SilentLogger:
    """Swallows log calls so a high-iteration test is not dominated by them."""

    def __getattr__(self, _name: str) -> Any:
        return lambda *args, **kwargs: None


class _FakeSubscriber:
    def __init__(self, futures: list[_FakeFuture]) -> None:
        self._futures = list(futures)
        self.calls: list[dict[str, Any]] = []

    @property
    def subscribe_calls(self) -> int:
        return len(self.calls)

    def subscribe(self, path: str, **kwargs: Any) -> _FakeFuture:
        self.calls.append({"path": path, **kwargs})
        return self._futures.pop(0)


class _RaisingSubscriber:
    """subscribe() itself fails, rather than the stream it returns."""

    def __init__(self, failures: int, then: _FakeFuture) -> None:
        self._remaining = failures
        self._then = then
        self.subscribe_calls = 0

    def subscribe(self, *_args: Any, **_kwargs: Any) -> _FakeFuture:
        self.subscribe_calls += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise RuntimeError("could not open the stream")
        return self._then


def _bare_sync() -> GmailSync:
    """A GmailSync with no clients.

    Built without __init__ deliberately: the real constructor opens Postgres,
    Pub/Sub and Gmail clients, none of which the behaviour under test depends on.
    """
    sync = object.__new__(GmailSync)
    sync.config = type(
        "_Config",
        (),
        {
            "subscription_path": "projects/p/subscriptions/s",
            "postgres": type("_Pg", (), {"pool_max_conn": 4})(),
        },
    )()
    sync._shutdown = GracefulShutdown()
    # The config the service actually runs on, not a copy of its literals: a
    # change to the real backoff must be able to fail these timing assertions.
    sync._stream_backoff_config = STREAM_BACKOFF_CONFIG
    sync._streaming_pull_future = None
    return sync


def _make_sync(futures: list[_FakeFuture]) -> GmailSync:
    sync = _bare_sync()
    sync.subscriber = _FakeSubscriber(futures)  # type: ignore[assignment]
    return sync


def _make_sync_stopping(errors: list[BaseException]) -> GmailSync:
    """A sync whose stream fails `errors` times, then closes for shutdown."""
    sync = _bare_sync()
    futures: list[_FakeFuture] = [_FakeFuture(e) for e in errors]
    futures.append(_StoppingFuture(sync._shutdown))
    sync.subscriber = _FakeSubscriber(futures)  # type: ignore[assignment]
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
    sync = _make_sync_stopping([error, error])

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == 3  # type: ignore[attr-defined]


def test_failing_subscribe_reconnects_rather_than_escaping() -> None:
    """subscribe() failing must not kill the process either.

    Opening the stream acquires credentials and builds a channel, so it raises on
    the same fault class as serving it. While the call sat outside the try, that
    exception escaped run() -- the pre-fix behaviour, on the second-most-likely
    failure point in the loop.
    """
    sync = _bare_sync()
    final = _StoppingFuture(sync._shutdown)
    sync.subscriber = _RaisingSubscriber(failures=2, then=final)  # type: ignore[assignment]

    with mock.patch.object(gmail_sync.time, "sleep"):
        sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == 3  # type: ignore[attr-defined]


def test_clean_close_while_running_reopens_the_stream(
    no_sleep: list[float],
) -> None:
    """A stream closing with no error, while serving, must not end the service.

    result() returns whenever the manager closes without an error. Treating that
    as shutdown would exit the process with "Listening on ..." as its last log
    line -- the silent death this loop exists to prevent.
    """
    sync = _bare_sync()
    # First future closes cleanly with the service still running; the second
    # closes because shutdown asked it to.
    futures: list[_FakeFuture] = [_FakeFuture(), _StoppingFuture(sync._shutdown)]
    sync.subscriber = _FakeSubscriber(futures)  # type: ignore[assignment]

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == 2  # type: ignore[attr-defined]


def test_flow_control_is_sized_to_the_connection_pool(no_sleep: list[float]) -> None:
    """Flow control must keep concurrent messages within the pool."""
    sync = _make_sync_stopping([])

    sync._serve_subscription()

    call = sync.subscriber.calls[0]  # type: ignore[attr-defined]
    assert call["path"] == "projects/p/subscriptions/s"
    assert call["flow_control"].max_messages == 4
    assert call["callback"] == sync.process_notification


def test_serve_subscription_publishes_the_future_for_shutdown(
    no_sleep: list[float],
) -> None:
    """Shutdown cancels through this attribute, so the loop must keep it current."""
    sync = _bare_sync()
    last = _StoppingFuture(sync._shutdown)
    sync.subscriber = _FakeSubscriber(  # type: ignore[assignment]
        [_FakeFuture(RuntimeError("stream died")), last]
    )

    sync._serve_subscription()

    assert sync._streaming_pull_future is last


def test_previous_stream_is_drained_before_resubscribing(
    no_sleep: list[float],
) -> None:
    """The reconnect must not run alongside the old stream's callbacks.

    subscribe() defaults to await_callbacks_on_shutdown=False, which shuts the
    callback executor with wait=False and resolves the future while callbacks are
    still running. Reconnecting then admits a second flow-control budget of
    messages against one connection pool, and the orphaned callbacks ack into a
    dispatcher that has already stopped -- so their messages come back on the new
    stream and are processed twice, racing on the sync_state checkpoint.
    """
    sync = _make_sync_stopping([RuntimeError("stream died")])

    sync._serve_subscription()

    for call in sync.subscriber.calls:  # type: ignore[attr-defined]
        assert call["await_callbacks_on_shutdown"] is True


def test_reconnect_backs_off_between_attempts(no_sleep: list[float]) -> None:
    """Repeated quick failures must wait longer each time, not reconnect hot."""
    sync = _make_sync_stopping([RuntimeError("stream died")] * 4)

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
    # Advance a full healthy interval on every call, so any two successive reads
    # differ by more than STREAM_HEALTHY_SECONDS. Deliberately independent of how
    # many times the loop reads the clock per iteration: an alternating fake
    # breaks the moment that count changes, and it already has once.
    step = gmail_sync.STREAM_HEALTHY_SECONDS + 1.0
    ticks = itertools.count()
    monkeypatch.setattr(gmail_sync.time, "monotonic", lambda: next(ticks) * step)
    monkeypatch.setattr(gmail_sync, "logger", _SilentLogger())

    sync = _make_sync_stopping([RuntimeError("stream died")] * 3)

    sync._serve_subscription()

    # Every failure followed a healthy stream, so every delay is the
    # first-attempt delay rather than an escalating one.
    ceiling = STREAM_BACKOFF_CONFIG.initial_delay * (1 + STREAM_BACKOFF_CONFIG.jitter)
    assert (
        max(no_sleep) <= ceiling
    ), f"backoff escalated across healthy streams: {no_sleep}"


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

    failures = 1100
    sync = _make_sync_stopping([RuntimeError("stream died")] * failures)

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == failures + 1  # type: ignore[attr-defined]
    assert len(no_sleep) == failures
    ceiling = STREAM_BACKOFF_CONFIG.max_delay * (1 + STREAM_BACKOFF_CONFIG.jitter)
    assert max(no_sleep) <= ceiling


def test_shutdown_stops_the_reconnect_loop(no_sleep: list[float]) -> None:
    """Once shutdown is requested the loop must not resubscribe."""
    sync = _make_sync([_FakeFuture(RuntimeError("stream died"))])
    sync._shutdown.initiate()

    sync._serve_subscription()

    assert sync.subscriber.subscribe_calls == 0  # type: ignore[attr-defined]


def test_shutdown_during_backoff_skips_the_sleep(no_sleep: list[float]) -> None:
    """A shutdown arriving mid-failure must not wait out the backoff first.

    Without the check after the failure is logged, a stopping service sleeps up
    to max_delay before noticing.
    """
    sync = _bare_sync()

    class _ShutdownOnCancel(_FakeFuture):
        def cancel(self) -> None:
            sync._shutdown.initiate()
            super().cancel()

    sync.subscriber = _FakeSubscriber(  # type: ignore[assignment]
        [_ShutdownOnCancel(RuntimeError("stream died"))]
    )

    sync._serve_subscription()

    assert no_sleep == [], "slept through a requested shutdown"
    assert sync.subscriber.subscribe_calls == 1  # type: ignore[attr-defined]


def test_reconnect_counters_increment(no_sleep: list[float]) -> None:
    """The counters are the only external signal that the loop is working.

    The incident narrative is that the container looked healthy throughout, so a
    metric that silently stopped incrementing would reproduce it.
    """
    before = gmail_sync.PUBSUB_STREAM_RECONNECTS._value.get()
    sync = _make_sync_stopping([RuntimeError("stream died")] * 2)

    sync._serve_subscription()

    after = gmail_sync.PUBSUB_STREAM_RECONNECTS._value.get()
    assert after - before == 2


def test_attempt_cap_keeps_the_delay_maths_finite() -> None:
    """The cap exists only to keep multiplier ** attempt finite."""
    multiplier = STREAM_BACKOFF_CONFIG.multiplier
    assert multiplier**STREAM_BACKOFF_ATTEMPT_CAP < float("inf")


def test_backoff_saturates_well_before_the_cap() -> None:
    """Capping the counter must not change the delay curve.

    If the cap were below the point where the curve reaches max_delay, it would
    silently shorten the longest backoff.
    """
    at_cap = gmail_sync.calculate_backoff_delay(
        STREAM_BACKOFF_ATTEMPT_CAP, STREAM_BACKOFF_CONFIG
    )
    assert at_cap >= STREAM_BACKOFF_CONFIG.max_delay


# --- shutdown ---------------------------------------------------------------------


def test_drain_and_close_cancels_the_live_stream() -> None:
    """Shutdown must stop the stream it was actually serving."""
    sync = _bare_sync()
    future = _FakeFuture()
    sync._streaming_pull_future = future

    sync._drain_and_close()

    assert future.cancelled


def test_drain_and_close_tolerates_never_having_subscribed() -> None:
    """Shutdown can arrive before the first subscribe() returns."""
    sync = _bare_sync()
    sync._streaming_pull_future = None

    sync._drain_and_close()  # must not raise


def test_drain_and_close_survives_a_future_that_reraises() -> None:
    """A stream that failed on the way out re-raises from result().

    The suppress() around it is what keeps that from escaping run() and turning
    a shutdown into a crash.
    """
    sync = _bare_sync()
    sync._streaming_pull_future = _FakeFuture(RuntimeError("stream had already failed"))

    sync._drain_and_close()  # must not raise


def test_signal_handlers_request_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    """SIGTERM must reach the shutdown flag.

    `docker stop` sends SIGTERM. With no handler the process was killed outright;
    the reconnect loop's should_continue() guards could never fire in production,
    since the only caller of initiate() ran on the same thread as the loop.
    """
    installed: dict[int, Any] = {}
    monkeypatch.setattr(
        gmail_sync.signal,
        "signal",
        lambda sig, handler: installed.__setitem__(sig, handler),
    )
    sync = _bare_sync()
    future = _FakeFuture()
    sync._streaming_pull_future = future

    sync._install_signal_handlers()

    assert set(installed) == {gmail_sync.signal.SIGTERM, gmail_sync.signal.SIGINT}
    installed[gmail_sync.signal.SIGTERM](gmail_sync.signal.SIGTERM, None)

    assert not sync._shutdown.should_continue()
    assert future.cancelled, "the live stream was not cancelled on SIGTERM"


def test_run_installs_signal_handlers_and_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run() must wire the handlers up and always drain on the way out.

    Testing _install_signal_handlers alone would pass even if run() never called
    it, leaving the loop's shutdown guards dead in production.
    """
    sync = _bare_sync()
    calls: list[str] = []

    monkeypatch.setattr(
        GmailSync, "_install_signal_handlers", lambda _s: calls.append("signals")
    )
    monkeypatch.setattr(GmailSync, "_check_watch_health", lambda _s: None)
    monkeypatch.setattr(GmailSync, "renew_watch", lambda _s: None)
    monkeypatch.setattr(GmailSync, "run_scheduler", lambda _s: None)
    monkeypatch.setattr(
        GmailSync, "_serve_subscription", lambda _s: calls.append("served")
    )
    monkeypatch.setattr(
        GmailSync, "_drain_and_close", lambda _s: calls.append("drained")
    )
    monkeypatch.setattr(gmail_sync.schedule, "every", lambda *a, **k: mock.MagicMock())

    sync.run()

    assert calls == ["signals", "served", "drained"]


def test_run_drains_after_an_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    """A KeyboardInterrupt must still reach the drain, not bypass it."""
    sync = _bare_sync()
    drained: list[bool] = []

    def _interrupt(_self: Any) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(GmailSync, "_install_signal_handlers", lambda _s: None)
    monkeypatch.setattr(GmailSync, "_check_watch_health", lambda _s: None)
    monkeypatch.setattr(GmailSync, "renew_watch", lambda _s: None)
    monkeypatch.setattr(GmailSync, "run_scheduler", lambda _s: None)
    monkeypatch.setattr(GmailSync, "_serve_subscription", _interrupt)
    monkeypatch.setattr(GmailSync, "_drain_and_close", lambda _s: drained.append(True))
    monkeypatch.setattr(gmail_sync.schedule, "every", lambda *a, **k: mock.MagicMock())

    sync.run()

    assert drained == [True]
    assert not sync._shutdown.should_continue()


# --- the notification retry path -------------------------------------------------


def _sync_for_notifications(max_retries: int = 2) -> GmailSync:
    sync = object.__new__(GmailSync)
    sync._backoff_config = BackoffConfig(
        initial_delay=0.0,
        max_delay=0.0,
        multiplier=1.0,
        jitter=0.0,
        max_retries=max_retries,
    )
    sync._shutdown = GracefulShutdown()
    return sync


@pytest.mark.parametrize(
    "error",
    [
        ssl.SSLError("UNEXPECTED_EOF_WHILE_READING"),
        BrokenPipeError(32, "Broken pipe"),
        ConnectionResetError(54, "Connection reset by peer"),
    ],
)
def test_transient_network_errors_are_retried_then_nacked(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    """These reached the nack path without a single retry.

    ssl.SSLError and BrokenPipeError descend from OSError without passing
    through urllib.error.URLError or TimeoutError, so the old tuple matched
    neither: retry_with_backoff re-raised on the first attempt while the log
    claimed the retries had been exhausted. Driving the real method rather than
    asserting the class hierarchy is what ties this to the code under change.
    """
    attempts = []

    def _boom(_self: Any, _message: Any) -> None:
        attempts.append(1)
        raise error

    monkeypatch.setattr(GmailSync, "_process_notification_inner", _boom)
    sync = _sync_for_notifications(max_retries=2)
    message = mock.Mock()

    sync._process_notification_with_retry(message)

    assert len(attempts) == 3, f"{type(error).__name__} was not retried"
    message.nack.assert_called_once()
    message.ack.assert_not_called()


def test_successful_notification_is_not_nacked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry wrapper must stay transparent on the happy path."""
    monkeypatch.setattr(
        GmailSync, "_process_notification_inner", lambda _self, _message: None
    )
    sync = _sync_for_notifications()
    message = mock.Mock()

    sync._process_notification_with_retry(message)

    message.nack.assert_not_called()


# --- stream failure diagnostics --------------------------------------------------


def _sync_with_token_expiry(expiry: Any) -> GmailSync:
    sync = _bare_sync()
    credentials = type("_Credentials", (), {"expiry": expiry})()
    transport = type("_Transport", (), {"_credentials": credentials})()
    sync.subscriber = type("_Subscriber", (), {"transport": transport})()  # type: ignore[assignment]
    return sync


def test_diagnostics_report_a_naive_token_expiry() -> None:
    """google-auth stores expiry naive; the arithmetic must not blow up on it.

    A missing replace(tzinfo=...) raises TypeError comparing naive to aware, and
    the broad except would swallow it into "unavailable" -- shipping green while
    the diagnostic never works.
    """
    expiry = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=90)
    sync = _sync_with_token_expiry(expiry)

    facts = sync._stream_failure_diagnostics()

    assert facts["token_expiry_utc"].endswith("+00:00")
    assert 80.0 < float(facts["token_seconds_remaining"]) <= 90.0


def test_diagnostics_report_an_aware_token_expiry() -> None:
    """An already-aware expiry must not be shifted by the conversion."""
    expiry = datetime.now(timezone.utc) + timedelta(seconds=90)
    sync = _sync_with_token_expiry(expiry)

    facts = sync._stream_failure_diagnostics()

    assert facts["token_expiry_utc"] == expiry.isoformat()
    assert 80.0 < float(facts["token_seconds_remaining"]) <= 90.0


def test_diagnostics_report_an_expired_token_as_negative() -> None:
    """The whole point is spotting a token that is already past its expiry."""
    expiry = datetime.now(timezone.utc) - timedelta(seconds=30)
    sync = _sync_with_token_expiry(expiry)

    facts = sync._stream_failure_diagnostics()

    assert float(facts["token_seconds_remaining"]) < 0


def test_diagnostics_distinguish_absent_expiry_from_unreadable() -> None:
    """ "No expiry set" and "could not read credentials" are different faults."""
    sync = _sync_with_token_expiry(None)

    facts = sync._stream_failure_diagnostics()

    assert facts["token_expiry_utc"] == "none"
    assert "token_seconds_remaining" not in facts


def test_diagnostics_never_raise_and_keep_the_reason() -> None:
    """Diagnostics run on the error path and must not mask the real fault.

    The credentials read is a private attribute that may move between library
    versions, so the failure keeps its reason rather than reading "unavailable"
    forever with no way to tell that from a missing token.
    """
    sync = _make_sync([])  # fake subscriber, no transport attribute

    facts = sync._stream_failure_diagnostics()

    assert "local_utc" in facts
    assert facts["token_expiry_utc"].startswith("unavailable: ")
