"""Tests for DuckDB client functions."""

import json
import urllib.error
import urllib.request
from unittest.mock import Mock, patch

import pytest

from postmark.storage.duckdb_client import (
    get_body_data_from_api,
    get_body_data_from_local,
)


class TestGetBodyDataFromApi:
    """Tests for get_body_data_from_api function."""

    @patch("urllib.request.urlopen")
    def test_successful_fetch(self, mock_urlopen: Mock) -> None:
        """Should fetch and parse body data successfully."""
        response_data = {"parts": [{"type": "text/plain"}], "body_data": "base64data"}
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = get_body_data_from_api("http://api:8081", "gmail123")

        assert result["parts"] == [{"type": "text/plain"}]
        assert result["body_data"] == "base64data"
        mock_urlopen.assert_called_once_with(
            "http://api:8081/body?gmail_id=gmail123", timeout=10
        )

    @patch("urllib.request.urlopen")
    def test_handles_404_not_found(self, mock_urlopen: Mock) -> None:
        """Should return None values for 404 (not found)."""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="", code=404, msg="Not Found", hdrs={}, fp=None
        )

        result = get_body_data_from_api("http://api:8081", "missing123")

        assert result["parts"] is None
        assert result["body_data"] is None

    @patch("urllib.request.urlopen")
    def test_raises_on_http_error(self, mock_urlopen: Mock) -> None:
        """Should raise HTTPError for non-404 HTTP errors."""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="", code=500, msg="Server Error", hdrs={}, fp=None
        )

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            get_body_data_from_api("http://api:8081", "gmail123")

        assert exc_info.value.code == 500

    @patch("urllib.request.urlopen")
    def test_raises_on_network_error(self, mock_urlopen: Mock) -> None:
        """Should raise URLError for network errors."""
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        with pytest.raises(urllib.error.URLError):
            get_body_data_from_api("http://api:8081", "gmail123")

    @patch("urllib.request.urlopen")
    def test_raises_on_timeout(self, mock_urlopen: Mock) -> None:
        """Should raise TimeoutError for request timeouts."""
        mock_urlopen.side_effect = TimeoutError("Request timed out")

        with pytest.raises(TimeoutError):
            get_body_data_from_api("http://api:8081", "gmail123")

    @patch("urllib.request.urlopen")
    def test_raises_on_invalid_json(self, mock_urlopen: Mock) -> None:
        """Should raise JSONDecodeError for invalid JSON responses."""
        mock_response = Mock()
        mock_response.read.return_value = b"not valid json"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        with pytest.raises(json.JSONDecodeError):
            get_body_data_from_api("http://api:8081", "gmail123")

    @patch("urllib.request.urlopen")
    def test_handles_missing_keys_in_response(self, mock_urlopen: Mock) -> None:
        """Should handle response with missing parts or body_data keys."""
        response_data = {}  # No parts or body_data
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(response_data).encode()
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = get_body_data_from_api("http://api:8081", "gmail123")

        assert result["parts"] is None
        assert result["body_data"] is None


class TestGetBodyDataFromLocal:
    """Tests for get_body_data_from_local function."""

    def test_returns_none_when_connection_is_none(self) -> None:
        """Should return None values when connection is None."""
        result = get_body_data_from_local(None, "gmail123")

        assert result["parts"] is None
        assert result["body_data"] is None

    def test_successful_fetch(self) -> None:
        """Should fetch and parse body data from local DuckDB."""
        parts_json = json.dumps([{"type": "text/plain", "content": "Hello"}])
        mock_result = (parts_json, "base64data")

        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = mock_result

        result = get_body_data_from_local(mock_conn, "gmail123")

        assert result["parts"] == [{"type": "text/plain", "content": "Hello"}]
        assert result["body_data"] == "base64data"
        mock_conn.execute.assert_called_once_with(
            "SELECT parts, body_data FROM email_bodies WHERE gmail_id = ?",
            ["gmail123"],
        )

    def test_returns_none_when_not_found(self) -> None:
        """Should return None values when email not found."""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = None

        result = get_body_data_from_local(mock_conn, "missing123")

        assert result["parts"] is None
        assert result["body_data"] is None

    def test_handles_null_parts(self) -> None:
        """Should handle NULL parts in database."""
        mock_result = (None, "base64data")
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = mock_result

        result = get_body_data_from_local(mock_conn, "gmail123")

        assert result["parts"] is None
        assert result["body_data"] == "base64data"

    def test_handles_null_body_data(self) -> None:
        """Should handle NULL body_data in database."""
        parts_json = json.dumps([{"type": "text/plain"}])
        mock_result = (parts_json, None)
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = mock_result

        result = get_body_data_from_local(mock_conn, "gmail123")

        assert result["parts"] == [{"type": "text/plain"}]
        assert result["body_data"] is None

    def test_parses_json_parts(self) -> None:
        """Should parse JSON parts for consistency with API."""
        parts_json = json.dumps(
            [
                {"type": "text/plain", "content": "Part 1"},
                {"type": "text/html", "content": "<p>Part 2</p>"},
            ]
        )
        mock_result = (parts_json, "base64data")
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = mock_result

        result = get_body_data_from_local(mock_conn, "gmail123")

        assert len(result["parts"]) == 2
        assert result["parts"][0]["type"] == "text/plain"
        assert result["parts"][1]["type"] == "text/html"
