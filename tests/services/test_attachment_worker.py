from unittest.mock import Mock

import pytest

from postmark.services.attachment_worker import AttachmentWorker


@pytest.fixture
def worker() -> AttachmentWorker:
    """Create a mock AttachmentWorker instance for testing methods."""
    # Create worker but don't call __init__ (would try to connect)
    worker = AttachmentWorker.__new__(AttachmentWorker)
    return worker


def test_find_attachment_in_parts_direct_match(worker: AttachmentWorker) -> None:
    """Test finding attachment when it's directly in the payload body."""
    payload = {
        "body": {"attachmentId": "abc123", "size": 1024},
        "filename": "test.pdf",
        "mimeType": "application/pdf",
    }

    result = worker._find_attachment_in_parts(payload, "abc123")

    assert result == {
        "filename": "test.pdf",
        "mime_type": "application/pdf",
        "size": 1024,
    }


def test_find_attachment_in_parts_nested(worker: AttachmentWorker) -> None:
    """Test finding attachment in nested MIME parts."""
    payload = {
        "body": {},
        "parts": [
            {"body": {}, "filename": "other.txt"},
            {
                "body": {"attachmentId": "xyz789", "size": 2048},
                "filename": "nested.jpg",
                "mimeType": "image/jpeg",
            },
        ],
    }

    result = worker._find_attachment_in_parts(payload, "xyz789")

    assert result == {
        "filename": "nested.jpg",
        "mime_type": "image/jpeg",
        "size": 2048,
    }


def test_find_attachment_in_parts_deeply_nested(worker: AttachmentWorker) -> None:
    """Test finding attachment in deeply nested MIME structure."""
    payload = {
        "body": {},
        "parts": [
            {
                "body": {},
                "parts": [
                    {
                        "body": {"attachmentId": "deep123", "size": 512},
                        "filename": "deep.zip",
                        "mimeType": "application/zip",
                    }
                ],
            }
        ],
    }

    result = worker._find_attachment_in_parts(payload, "deep123")

    assert result == {
        "filename": "deep.zip",
        "mime_type": "application/zip",
        "size": 512,
    }


def test_find_attachment_in_parts_not_found(worker: AttachmentWorker) -> None:
    """Test that None is returned when attachment ID is not found."""
    payload = {
        "body": {"attachmentId": "other123"},
        "parts": [{"body": {"attachmentId": "another456"}}],
    }

    result = worker._find_attachment_in_parts(payload, "missing999")

    assert result is None


def test_find_attachment_in_parts_missing_fields(worker: AttachmentWorker) -> None:
    """Test default values when filename/mimeType/size are missing."""
    payload = {
        "body": {"attachmentId": "test123"}
        # No filename, mimeType, or size fields
    }

    result = worker._find_attachment_in_parts(payload, "test123")

    assert result == {
        "filename": "attachment",
        "mime_type": "application/octet-stream",
        "size": 0,
    }


def test_find_attachment_in_parts_empty_payload(worker: AttachmentWorker) -> None:
    """Test behavior with empty payload."""
    payload = {}

    result = worker._find_attachment_in_parts(payload, "any123")

    assert result is None


def test_find_attachment_in_parts_no_parts(worker: AttachmentWorker) -> None:
    """Test payload with no parts array."""
    payload = {
        "body": {"attachmentId": "other123"},
        # No 'parts' key
    }

    result = worker._find_attachment_in_parts(payload, "target123")

    assert result is None
