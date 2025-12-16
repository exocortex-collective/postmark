"""Reliability utilities for Cortex services.

Provides circuit breaker, exponential backoff, and graceful shutdown helpers.
"""

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5  # Failures before opening circuit
    recovery_timeout: float = 30.0  # Seconds before testing recovery
    half_open_max_calls: int = 1  # Max calls in half-open state


class CircuitBreaker:
    """Circuit breaker pattern for protecting against cascade failures.

    When a service fails repeatedly, the circuit opens and subsequent calls
    fail fast without attempting the operation. After a recovery timeout,
    the circuit enters half-open state to test if the service recovered.

    Thread-safe implementation using locks.
    """

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> None:
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float = 0
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state (may transition on access)."""
        with self._lock:
            return self._get_state_locked()

    def _get_state_locked(self) -> CircuitState:
        """Get current state, transitioning from OPEN to HALF_OPEN if timeout passed."""
        if self._state == CircuitState.OPEN:
            if (
                time.monotonic() - self._last_failure_time
                >= self.config.recovery_timeout
            ):
                logger.info(
                    f"Circuit {self.name}: OPEN -> HALF_OPEN (recovery timeout)"
                )
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    def allow_request(self) -> bool:
        """Check if a request should be allowed through."""
        with self._lock:
            state = self._get_state_locked()
            if state == CircuitState.CLOSED:
                return True
            if state == CircuitState.OPEN:
                return False
            # HALF_OPEN - allow limited calls
            if self._half_open_calls < self.config.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                logger.info(f"Circuit {self.name}: HALF_OPEN -> CLOSED (success)")
                self._state = CircuitState.CLOSED
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning(f"Circuit {self.name}: HALF_OPEN -> OPEN (failure)")
                self._state = CircuitState.OPEN
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.config.failure_threshold
            ):
                logger.warning(
                    f"Circuit {self.name}: CLOSED -> OPEN "
                    f"({self._failure_count} consecutive failures)"
                )
                self._state = CircuitState.OPEN


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open and rejecting calls."""

    pass


def call_with_circuit_breaker(
    circuit: CircuitBreaker,
    func: Callable[[], T],
) -> T:
    """Execute a function with circuit breaker protection.

    Args:
        circuit: The circuit breaker instance
        func: Function to execute

    Returns:
        The function's return value

    Raises:
        CircuitOpenError: If circuit is open
        Any exception from func: After recording failure
    """
    if not circuit.allow_request():
        raise CircuitOpenError(f"Circuit {circuit.name} is open")

    try:
        result = func()
        circuit.record_success()
        return result
    except Exception:
        circuit.record_failure()
        raise


@dataclass
class BackoffConfig:
    """Configuration for exponential backoff."""

    initial_delay: float = 1.0  # Initial delay in seconds
    max_delay: float = 60.0  # Maximum delay
    multiplier: float = 2.0  # Delay multiplier per retry
    jitter: float = 0.1  # Random jitter factor (0-1)
    max_retries: int = 3  # Max retry attempts

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")


def calculate_backoff_delay(attempt: int, config: BackoffConfig) -> float:
    """Calculate delay for a given retry attempt (0-indexed).

    Uses exponential backoff with jitter to prevent thundering herd.
    """
    delay = min(
        config.initial_delay * (config.multiplier**attempt),
        config.max_delay,
    )
    # Add random jitter
    jitter = delay * config.jitter * random.random()
    return delay + jitter


def retry_with_backoff(
    func: Callable[[], T],
    config: BackoffConfig | None = None,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """Execute a function with exponential backoff retry.

    Args:
        func: Function to execute
        config: Backoff configuration
        retryable_exceptions: Exception types that trigger retry

    Returns:
        The function's return value

    Raises:
        The last exception if all retries exhausted
    """
    config = config or BackoffConfig()
    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return func()
        except retryable_exceptions as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = calculate_backoff_delay(attempt, config)
                logger.warning(
                    f"Attempt {attempt + 1}/{config.max_retries + 1} failed: {e}. "
                    f"Retrying in {delay:.2f}s"
                )
                time.sleep(delay)

    # last_exception is guaranteed to be set since max_retries >= 0 (validated in __post_init__)
    assert last_exception is not None
    raise last_exception


@dataclass
class GracefulShutdown:
    """Helper for graceful shutdown with in-flight tracking.

    Usage:
        shutdown = GracefulShutdown()

        # In signal handler:
        shutdown.initiate()

        # In main loop:
        while shutdown.should_continue():
            with shutdown.track_work():
                process_job()

        # Wait for completion:
        shutdown.wait_for_completion(timeout=30)
    """

    _shutdown_requested: bool = field(default=False, init=False)
    _in_flight: int = field(default=0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _condition: threading.Condition = field(init=False)

    def __post_init__(self) -> None:
        self._condition = threading.Condition(self._lock)

    def initiate(self) -> None:
        """Signal that shutdown has been requested."""
        with self._lock:
            self._shutdown_requested = True
            logger.info("Graceful shutdown initiated")

    def should_continue(self) -> bool:
        """Check if the service should continue accepting new work."""
        with self._lock:
            return not self._shutdown_requested

    def track_work(self) -> "WorkTracker":
        """Context manager to track in-flight work."""
        return WorkTracker(self)

    def _increment(self) -> None:
        with self._lock:
            self._in_flight += 1

    def _decrement(self) -> None:
        with self._lock:
            self._in_flight -= 1
            if self._in_flight == 0:
                self._condition.notify_all()

    def wait_for_completion(self, timeout: float = 30.0) -> bool:
        """Wait for all in-flight work to complete.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            True if all work completed, False if timeout reached
        """
        with self._condition:
            end_time = time.monotonic() + timeout
            while self._in_flight > 0:
                remaining = end_time - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        f"Shutdown timeout: {self._in_flight} jobs still in flight"
                    )
                    return False
                logger.info(f"Waiting for {self._in_flight} in-flight jobs...")
                self._condition.wait(timeout=min(remaining, 5.0))
            logger.info("All in-flight work completed")
            return True


class WorkTracker:
    """Context manager for tracking in-flight work."""

    def __init__(self, shutdown: GracefulShutdown) -> None:
        self._shutdown = shutdown

    def __enter__(self) -> "WorkTracker":
        self._shutdown._increment()
        return self

    def __exit__(self, *args: object) -> None:
        self._shutdown._decrement()
