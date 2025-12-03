"""DuckDB client for fetching email bodies.

Shared client used by parse_worker and test scripts.
Supports both REST API access and direct local DuckDB connections.
"""

import json
import logging
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


def get_body_data_from_api(api_url: str, gmail_id: str) -> dict[str, Any]:
    """Fetch body data from DuckDB REST API.

    Args:
        api_url: Base URL of the DuckDB API (e.g., http://localhost:8081)
        gmail_id: Gmail message ID to fetch body for

    Returns:
        Dict with 'parts' and 'body_data' keys. Values are None if not found (404).

    Raises:
        urllib.error.HTTPError: For non-404 HTTP errors
        urllib.error.URLError: For network errors
        TimeoutError: For request timeouts
        json.JSONDecodeError: For invalid JSON responses
    """
    url = f"{api_url}/body?gmail_id={gmail_id}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return {"parts": data.get("parts"), "body_data": data.get("body_data")}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            # Not found is expected for missing bodies
            return {"parts": None, "body_data": None}
        logger.warning(f"HTTP error fetching body for {gmail_id}: {e}")
        raise
    except (urllib.error.URLError, TimeoutError) as e:
        logger.warning(f"Network error fetching body for {gmail_id}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON response for {gmail_id}: {e}")
        raise


def get_body_data_from_local(duck_conn: Any, gmail_id: str) -> dict[str, Any]:
    """Fetch body data from local DuckDB connection.

    Args:
        duck_conn: DuckDB connection object
        gmail_id: Gmail message ID to fetch body for

    Returns:
        Dict with 'parts' and 'body_data' keys. Values are None if not found.
        Parts are parsed from JSON for consistency with API response.
    """
    if duck_conn is None:
        return {"parts": None, "body_data": None}
    result = duck_conn.execute(
        "SELECT parts, body_data FROM email_bodies WHERE gmail_id = ?",
        [gmail_id],
    ).fetchone()
    if result:
        # Parse JSON parts for consistency with API response
        parts_data = json.loads(result[0]) if result[0] else None
        return {"parts": parts_data, "body_data": result[1]}
    return {"parts": None, "body_data": None}
