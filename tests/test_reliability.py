"""Tests for reliability utilities (circuit breaker, backoff, graceful shutdown)."""

import threading
import time
from unittest.mock import Mock

import pytest

from postmark.reliability import (
    BackoffConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    GracefulShutdown,
    calculate_backoff_delay,
    call_with_circuit_breaker,
    retry_with_backoff,
)


class TestCircuitBreaker:
    """Tests for CircuitBreaker pattern."""

    def test_initial_state_is_closed(self) -> None:
        """Circuit should start in CLOSED state."""
        cb = CircuitBreaker(name="test")
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_opens_after_threshold_failures(self) -> None:
        """Circuit should open after reaching failure threshold."""
        cb = CircuitBreaker(
            name="test", config=CircuitBreakerConfig(failure_threshold=3)
        )

        # Record 2 failures - should stay closed
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

        # 3rd failure should open circuit
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    def test_success_resets_failure_count(self) -> None:
        """Success should reset failure count in CLOSED state."""
        cb = CircuitBreaker(
            name="test", config=CircuitBreakerConfig(failure_threshold=3)
        )

        cb.record_failure()
        cb.record_failure()
        cb.record_success()  # Reset

        # Should need 3 more failures to open
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED

        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_transitions_to_half_open_after_timeout(self) -> None:
        """Circuit should transition from OPEN to HALF_OPEN after recovery timeout."""
        cb = CircuitBreaker(
            name="test",
            config=CircuitBreakerConfig(
                failure_threshold=2, recovery_timeout=0.1  # 100ms timeout
            ),
        )

        # Open the circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.15)

        # Should transition to HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_allows_limited_requests(self) -> None:
        """HALF_OPEN state should allow limited test requests."""
        cb = CircuitBreaker(
            name="test",
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.1,
                half_open_max_calls=2,
            ),
        )

        # Open and wait for half-open
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

        # Should allow up to 2 requests
        assert cb.allow_request() is True
        assert cb.allow_request() is True
        assert cb.allow_request() is False  # Limit reached

    def test_half_open_closes_on_success(self) -> None:
        """Success in HALF_OPEN should close the circuit."""
        cb = CircuitBreaker(
            name="test",
            config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1),
        )

        # Open and wait for half-open
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

        # Success should close circuit
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_half_open_opens_on_failure(self) -> None:
        """Failure in HALF_OPEN should reopen the circuit."""
        cb = CircuitBreaker(
            name="test",
            config=CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.1),
        )

        # Open and wait for half-open
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

        # Failure should reopen circuit
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    def test_thread_safety(self) -> None:
        """Circuit breaker should be thread-safe."""
        cb = CircuitBreaker(
            name="test", config=CircuitBreakerConfig(failure_threshold=10)
        )
        results = []

        def worker() -> None:
            for _ in range(5):
                cb.record_failure()
                results.append(cb.allow_request())

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should eventually open after 10 failures
        assert cb.state == CircuitState.OPEN


class TestCallWithCircuitBreaker:
    """Tests for call_with_circuit_breaker helper."""

    def test_executes_function_when_closed(self) -> None:
        """Should execute function when circuit is closed."""
        cb = CircuitBreaker(name="test")
        func = Mock(return_value="success")

        result = call_with_circuit_breaker(cb, func)

        assert result == "success"
        func.assert_called_once()
        assert cb.state == CircuitState.CLOSED

    def test_raises_when_circuit_open(self) -> None:
        """Should raise CircuitOpenError when circuit is open."""
        cb = CircuitBreaker(
            name="test", config=CircuitBreakerConfig(failure_threshold=1)
        )
        cb.record_failure()  # Open circuit
        func = Mock()

        with pytest.raises(CircuitOpenError, match="Circuit test is open"):
            call_with_circuit_breaker(cb, func)

        func.assert_not_called()

    def test_records_success_on_success(self) -> None:
        """Should record success when function succeeds."""
        cb = CircuitBreaker(
            name="test", config=CircuitBreakerConfig(failure_threshold=2)
        )
        func = Mock(return_value="success")

        # Record a failure first
        try:
            call_with_circuit_breaker(cb, Mock(side_effect=ValueError()))
        except ValueError:
            pass

        # Success should reset failure count (verify by checking state remains closed)
        call_with_circuit_breaker(cb, func)
        assert cb.state == CircuitState.CLOSED

        # One more failure shouldn't open circuit (count was reset)
        try:
            call_with_circuit_breaker(cb, Mock(side_effect=ValueError()))
        except ValueError:
            pass
        assert cb.state == CircuitState.CLOSED  # Still closed (count was reset)

    def test_records_failure_and_raises_on_exception(self) -> None:
        """Should record failure and re-raise exception."""
        cb = CircuitBreaker(
            name="test", config=CircuitBreakerConfig(failure_threshold=2)
        )
        func = Mock(side_effect=ValueError("test error"))

        # First failure - circuit should stay closed
        with pytest.raises(ValueError, match="test error"):
            call_with_circuit_breaker(cb, func)
        assert cb.state == CircuitState.CLOSED

        # Second failure should open circuit
        with pytest.raises(ValueError):
            call_with_circuit_breaker(cb, func)

        assert cb.state == CircuitState.OPEN


class TestBackoff:
    """Tests for exponential backoff."""

    def test_calculate_backoff_delay_exponential(self) -> None:
        """Delay should increase exponentially."""
        config = BackoffConfig(initial_delay=1.0, multiplier=2.0, jitter=0.0)

        delay0 = calculate_backoff_delay(0, config)
        delay1 = calculate_backoff_delay(1, config)
        delay2 = calculate_backoff_delay(2, config)

        assert delay0 == 1.0  # 1 * 2^0
        assert delay1 == 2.0  # 1 * 2^1
        assert delay2 == 4.0  # 1 * 2^2

    def test_calculate_backoff_delay_respects_max(self) -> None:
        """Delay should not exceed max_delay."""
        config = BackoffConfig(
            initial_delay=10.0, multiplier=2.0, max_delay=15.0, jitter=0.0
        )

        delay2 = calculate_backoff_delay(2, config)  # Would be 40 without max
        assert delay2 == 15.0

    def test_calculate_backoff_delay_adds_jitter(self) -> None:
        """Delay should include random jitter."""
        config = BackoffConfig(initial_delay=10.0, multiplier=1.0, jitter=0.1)

        delays = [calculate_backoff_delay(0, config) for _ in range(10)]

        # All should be >= 10.0 (base delay)
        assert all(d >= 10.0 for d in delays)
        # At least some should differ (jitter adds randomness)
        assert len(set(delays)) > 1

    def test_backoff_config_validates_max_retries(self) -> None:
        """BackoffConfig should reject negative max_retries."""
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            BackoffConfig(max_retries=-1)


class TestRetryWithBackoff:
    """Tests for retry_with_backoff."""

    def test_succeeds_on_first_attempt(self) -> None:
        """Should return immediately on success."""
        func = Mock(return_value="success")
        config = BackoffConfig(max_retries=3)

        result = retry_with_backoff(func, config)

        assert result == "success"
        func.assert_called_once()

    def test_retries_on_failure(self) -> None:
        """Should retry on retryable exceptions."""
        func = Mock(side_effect=[ValueError("fail"), ValueError("fail"), "success"])
        config = BackoffConfig(initial_delay=0.01, max_retries=2)  # Fast for testing

        result = retry_with_backoff(func, config)

        assert result == "success"
        assert func.call_count == 3

    def test_raises_after_max_retries(self) -> None:
        """Should raise last exception after exhausting retries."""
        func = Mock(side_effect=ValueError("persistent failure"))
        config = BackoffConfig(initial_delay=0.01, max_retries=2)

        with pytest.raises(ValueError, match="persistent failure"):
            retry_with_backoff(func, config)

        assert func.call_count == 3  # Initial + 2 retries

    def test_respects_retryable_exceptions(self) -> None:
        """Should only retry on specified exception types."""
        func = Mock(side_effect=KeyError("not retryable"))
        config = BackoffConfig(initial_delay=0.01, max_retries=2)

        # KeyError is not in default retryable_exceptions
        with pytest.raises(KeyError, match="not retryable"):
            retry_with_backoff(func, config, retryable_exceptions=(ValueError,))

        # Should not retry - fails immediately
        func.assert_called_once()

    def test_delays_between_retries(self) -> None:
        """Should delay between retry attempts."""
        func = Mock(side_effect=[ValueError("fail"), "success"])
        config = BackoffConfig(initial_delay=0.05, max_retries=1)

        start = time.monotonic()
        retry_with_backoff(func, config)
        elapsed = time.monotonic() - start

        # Should have delayed at least 50ms
        assert elapsed >= 0.05


class TestGracefulShutdown:
    """Tests for GracefulShutdown helper."""

    def test_initial_state_allows_work(self) -> None:
        """Should allow work before shutdown initiated."""
        shutdown = GracefulShutdown()
        assert shutdown.should_continue() is True

    def test_initiate_stops_new_work(self) -> None:
        """Initiating shutdown should stop accepting new work."""
        shutdown = GracefulShutdown()
        shutdown.initiate()
        assert shutdown.should_continue() is False

    def test_tracks_in_flight_work(self) -> None:
        """Should track in-flight work count."""
        shutdown = GracefulShutdown()

        # Verify wait_for_completion behavior with nested work tracking
        def nested_worker() -> None:
            with shutdown.track_work():
                time.sleep(0.05)
                with shutdown.track_work():
                    time.sleep(0.05)

        thread = threading.Thread(target=nested_worker)
        thread.start()

        # Should wait for all nested work to complete
        assert shutdown.wait_for_completion(timeout=1.0) is True
        thread.join()

    def test_wait_for_completion_succeeds(self) -> None:
        """Should wait for all work to complete."""
        shutdown = GracefulShutdown()

        def worker() -> None:
            with shutdown.track_work():
                time.sleep(0.1)

        thread = threading.Thread(target=worker)
        thread.start()

        # Wait should succeed
        assert shutdown.wait_for_completion(timeout=1.0) is True
        thread.join()

    def test_wait_for_completion_times_out(self) -> None:
        """Should timeout if work doesn't complete."""
        shutdown = GracefulShutdown()

        def worker() -> None:
            with shutdown.track_work():
                time.sleep(1.0)  # Longer than timeout

        thread = threading.Thread(target=worker)
        thread.start()

        # Wait should timeout
        assert shutdown.wait_for_completion(timeout=0.1) is False
        thread.join()

    def test_typical_shutdown_workflow(self) -> None:
        """Test typical shutdown workflow."""
        shutdown = GracefulShutdown()

        # Simulate work loop
        work_items = []

        def worker() -> None:
            while shutdown.should_continue():
                with shutdown.track_work():
                    work_items.append(1)
                    time.sleep(0.05)

        thread = threading.Thread(target=worker)
        thread.start()

        # Let some work happen
        time.sleep(0.15)

        # Initiate shutdown
        shutdown.initiate()

        # Wait for completion
        assert shutdown.wait_for_completion(timeout=1.0) is True
        thread.join()

        # Should have processed some items
        assert len(work_items) > 0
