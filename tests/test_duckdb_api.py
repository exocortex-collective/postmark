"""Tests for DuckDB API endpoints."""

import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def test_db():
    """Create a temporary DuckDB database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        os.environ["DUCKDB_PATH"] = str(db_path)
        # Force reload of the module to pick up new env var
        import importlib

        from postmark.services import duckdb_api

        importlib.reload(duckdb_api)
        yield duckdb_api.app
        # Cleanup
        if db_path.exists():
            db_path.unlink()


@pytest.fixture
def client(test_db):
    """Create a test client."""
    return TestClient(test_db)


@pytest.fixture
def populated_db_client(client):
    """Client with a DB pre-populated with 15 test emails."""
    for i in range(15):
        client.post(
            "/body",
            json={
                "gmail_id": f"test{i:03d}",
                "parts": [
                    {"mimeType": "text/plain", "body": {"data": "SGVsbG8gV29ybGQ="}}
                ],
                "body_data": None,
            },
        )
    return client


def test_health_check(client):
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_stats_empty(client):
    """Test stats endpoint with empty database."""
    response = client.get("/stats")
    assert response.status_code == 200
    assert response.json()["count"] == 0


def test_store_and_get_body(client):
    """Test storing and retrieving a single email body."""
    # Store a body
    body_data = {
        "gmail_id": "test123",
        "parts": [{"mimeType": "text/plain", "body": {"data": "SGVsbG8gV29ybGQ="}}],
        "body_data": None,
    }
    response = client.post("/body", json=body_data)
    assert response.status_code == 201
    assert response.json() == {"status": "ok"}

    # Retrieve it
    response = client.get("/body", params={"gmail_id": "test123"})
    assert response.status_code == 200
    data = response.json()
    assert data["gmail_id"] == "test123"
    assert data["parts"] is not None
    assert data["body_text"] == "Hello World"


def test_gmail_ids_default_limit(populated_db_client, monkeypatch):
    """Test /gmail_ids endpoint applies default limit."""
    # Patch the default limit to a small number for testing
    import postmark.services.duckdb_api

    monkeypatch.setattr(postmark.services.duckdb_api, "GMAIL_IDS_DEFAULT_LIMIT", 5)

    # Without limit param, should use the patched default limit (5)
    response = populated_db_client.get("/gmail_ids")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 5
    assert len(data["gmail_ids"]) == 5

    # Verify it's actually enforcing the limit (we have 15 records total)
    # Should only get 5 even though more are available
    assert data["count"] < 15


def test_gmail_ids_explicit_limit(populated_db_client):
    """Test /gmail_ids endpoint respects explicit limit parameter."""
    # Explicit limit=5
    response = populated_db_client.get("/gmail_ids", params={"limit": 5})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 5
    assert len(data["gmail_ids"]) == 5


def test_gmail_ids_unlimited(populated_db_client):
    """Test /gmail_ids endpoint with limit=0 returns all records."""
    # Explicit limit=0 should return all
    response = populated_db_client.get("/gmail_ids", params={"limit": 0})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 15
    assert len(data["gmail_ids"]) == 15


def test_gmail_ids_pagination(populated_db_client):
    """Test /gmail_ids endpoint pagination with offset."""
    # First page
    response = populated_db_client.get("/gmail_ids", params={"limit": 5, "offset": 0})
    assert response.status_code == 200
    page1 = response.json()
    assert page1["count"] == 5

    # Second page
    response = populated_db_client.get("/gmail_ids", params={"limit": 5, "offset": 5})
    assert response.status_code == 200
    page2 = response.json()
    assert page2["count"] == 5

    # Pages should have different IDs
    assert set(page1["gmail_ids"]) != set(page2["gmail_ids"])


def test_gmail_ids_unlimited_with_offset(populated_db_client):
    """Test /gmail_ids with limit=0 (unlimited) respects offset and returns sorted results."""
    # Get all IDs from the beginning
    response_all = populated_db_client.get("/gmail_ids", params={"limit": 0})
    assert response_all.status_code == 200
    all_ids = response_all.json()["gmail_ids"]

    # Results should be ordered by gmail_id
    assert all_ids == sorted(all_ids)

    # Get all IDs starting from offset 10
    response_offset = populated_db_client.get(
        "/gmail_ids", params={"limit": 0, "offset": 10}
    )
    assert response_offset.status_code == 200
    offset_ids = response_offset.json()["gmail_ids"]

    # Should get 5 IDs (15 total - 10 offset)
    assert len(offset_ids) == 5

    # The offset IDs should match the last 5 from all IDs (and be sorted)
    assert offset_ids == all_ids[10:]
    assert offset_ids == sorted(offset_ids)

    # Verify deterministic results - expected sorted IDs based on fixture
    expected_ids = [f"test{i:03d}" for i in range(10, 15)]
    assert offset_ids == expected_ids


def test_gmail_ids_negative_params_rejected(client):
    """Test /gmail_ids rejects negative limit and offset parameters."""
    # Negative limit should return 422 Unprocessable Entity
    response = client.get("/gmail_ids", params={"limit": -1})
    assert response.status_code == 422

    # Negative offset should return 422
    response = client.get("/gmail_ids", params={"offset": -1})
    assert response.status_code == 422


def test_body_not_found(client):
    """Test retrieving non-existent body returns 404."""
    response = client.get("/body", params={"gmail_id": "nonexistent"})
    assert response.status_code == 404
    assert "No body found" in response.json()["detail"]


def test_mail_text(client):
    """Test /mail_text endpoint returns formatted text."""
    # Store a body with HTML
    body_data = {
        "gmail_id": "test123",
        "parts": [
            {
                "mimeType": "text/html",
                "body": {"data": "PGI+SGVsbG88L2I+IFdvcmxk"},  # <b>Hello</b> World
            }
        ],
        "body_data": None,
    }
    response = client.post("/body", json=body_data)
    assert response.status_code == 201

    # Retrieve formatted text
    response = client.get("/mail_text", params={"gmail_id": "test123"})
    assert response.status_code == 200
    data = response.json()
    assert data["gmail_id"] == "test123"
    assert "**Hello**" in data["text"]  # HTML should be converted to markdown


def test_bodies_batch(client):
    """Test /bodies endpoint batch retrieval."""
    # Store 3 bodies
    for i in range(3):
        client.post(
            "/body",
            json={
                "gmail_id": f"test{i}",
                "parts": [
                    {"mimeType": "text/plain", "body": {"data": "SGVsbG8gV29ybGQ="}}
                ],
                "body_data": None,
            },
        )

    # Batch retrieve
    response = client.get("/bodies", params={"gmail_ids": "test0,test1,test2"})
    assert response.status_code == 200
    data = response.json()
    assert len(data["bodies"]) == 3
    assert "test0" in data["bodies"]
    assert "test1" in data["bodies"]
    assert "test2" in data["bodies"]


def test_bodies_max_limit(client):
    """Test /bodies endpoint enforces max 100 IDs."""
    # Try to request 101 IDs
    gmail_ids = ",".join([f"test{i}" for i in range(101)])
    response = client.get("/bodies", params={"gmail_ids": gmail_ids})
    assert response.status_code == 400
    assert "Max 100 gmail_ids" in response.json()["detail"]


def test_mail_text_converts_mislabelled_html_in_text_plain(client):
    """A text/plain part that is actually HTML must still be converted.

    This branch had no coverage: forcing _looks_like_html to return False left
    all 257 tests green, so nothing pinned the mislabelled-HTML path.
    """
    client.post(
        "/body",
        json={
            "gmail_id": "mislabelled1",
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {
                        "data": "PGRpdiBzdHlsZT0iY29sb3I6IzMzMyI-PHRhYmxlIGNlbGxwYWRkaW5nPSIwIj48dHI-PHRkPjxiPlJldW5pJm9hY3V0ZTtuPC9iPiB3aXRoIEpvcyYjMjMzOzwvdGQ-PC90cj48L3RhYmxlPjwvZGl2Pg=="
                    },
                }
            ],
            "body_data": None,
        },
    )
    text = client.get("/mail_text", params={"gmail_id": "mislabelled1"}).json()["text"]
    # The conversion happened (markup gone, emphasis rendered) and the accented
    # character survived as itself rather than being flattened to "o".
    assert "<table" not in text and "<div" not in text
    assert "ón" in text and "José" in text, text
    # KNOWN UPSTREAM DEFECT (cortex-1erc): html2text inserts a spurious space
    # when an entity sits inside an emphasis tag, so this arrives as
    # "**Reuni ón**". Asserted as-is so the day html2text fixes it, this test
    # fails and tells us rather than silently drifting.
    assert "**Reuni ón**" in text, text


def test_mail_text_converts_html_in_body_data_without_parts(client):
    """Some senders put HTML in body_data with no structured parts at all."""
    client.post(
        "/body",
        json={
            "gmail_id": "mislabelled2",
            "parts": [],
            "body_data": "PGRpdiBzdHlsZT0iY29sb3I6IzMzMyI-PHRhYmxlIGNlbGxwYWRkaW5nPSIwIj48dHI-PHRkPjxiPlJldW5pJm9hY3V0ZTtuPC9iPiB3aXRoIEpvcyYjMjMzOzwvdGQ-PC90cj48L3RhYmxlPjwvZGl2Pg==",
        },
    )
    text = client.get("/mail_text", params={"gmail_id": "mislabelled2"}).json()["text"]
    assert "José" in text, text
    assert "cellpadding" not in text


def test_mail_text_leaves_genuine_plain_text_alone(client):
    """The other side of the heuristic: real prose must not be mangled.

    Asserts EXACT equality, not a substring. A substring assertion passes even
    when the text is wrongly run through html2text, because prose survives the
    round trip nearly intact -- so it proved nothing. The paragraph break is
    what actually discriminates: conversion collapses "a,\n\nb" to "a, b".
    """
    plain = "Dear families,\n\nConferences are on Tuesday. Sign up via the office.\n"
    client.post(
        "/body",
        json={
            "gmail_id": "genuinely-plain",
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {
                        "data": "RGVhciBmYW1pbGllcywKCkNvbmZlcmVuY2VzIGFyZSBvbiBUdWVzZGF5LiBTaWduIHVwIHZpYSB0aGUgb2ZmaWNlLgo="
                    },
                }
            ],
            "body_data": None,
        },
    )
    text = client.get("/mail_text", params={"gmail_id": "genuinely-plain"}).json()[
        "text"
    ]
    assert text == plain, f"plain text was altered: {text!r}"
