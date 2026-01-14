from unittest.mock import Mock

from googleapiclient.errors import HttpError

from postmark.services.gmail_sync import is_retryable_http_error


def test_is_retryable_http_error_5xx() -> None:
    """Test that 5XX server errors are retryable."""
    for status in [500, 502, 503, 504]:
        resp = Mock()
        resp.status = status
        error = HttpError(resp=resp, content=b"Server error")
        assert is_retryable_http_error(error), f"Status {status} should be retryable"


def test_is_retryable_http_error_429() -> None:
    """Test that 429 rate limit is retryable."""
    resp = Mock()
    resp.status = 429
    error = HttpError(resp=resp, content=b"Rate limit exceeded")
    assert is_retryable_http_error(error), "429 rate limit should be retryable"


def test_is_retryable_http_error_4xx_not_retryable() -> None:
    """Test that 4XX client errors (except 429) are not retryable."""
    for status in [400, 401, 403, 404, 409]:
        resp = Mock()
        resp.status = status
        error = HttpError(resp=resp, content=b"Client error")
        assert not is_retryable_http_error(
            error
        ), f"Status {status} should not be retryable"


def test_is_retryable_http_error_2xx_retryable() -> None:
    """Test that 2XX success codes are conservatively retryable."""
    resp = Mock()
    resp.status = 200
    error = HttpError(resp=resp, content=b"Success")
    assert is_retryable_http_error(
        error
    ), "Non-4XX/5XX should be conservatively retryable"


def test_is_retryable_http_error_3xx_retryable() -> None:
    """Test that 3XX redirect codes are conservatively retryable."""
    resp = Mock()
    resp.status = 301
    error = HttpError(resp=resp, content=b"Redirect")
    assert is_retryable_http_error(error), "3XX should be conservatively retryable"


def test_is_retryable_http_error_unusual_status() -> None:
    """Test that unusual status codes are conservatively retryable."""
    resp = Mock()
    resp.status = 999
    error = HttpError(resp=resp, content=b"Unknown")
    assert is_retryable_http_error(
        error
    ), "Unusual status should be conservatively retryable"
