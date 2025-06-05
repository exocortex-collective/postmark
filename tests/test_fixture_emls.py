from pathlib import Path

import pytest
from datetime import datetime

from postmark.email.parser import parse_eml


# Dictionary of per-file test expectations.
# "key" should match the base name (stem) of the .eml file
# "value" is a dict of expected fields.
# For body_text, body_html, attachments, store expected lengths (or set them to None to skip).
TEST_EMAIL_EXPECTATIONS = {
    "example1": {
        "message_id": "<abc123@example.com>",
        "subject": "Hello from example1",
        "from_addr": "sender@example.com",
        "date": datetime(2023, 10, 11, 8, 30, 0),
        "body_text_len": 42,
        "body_html_len": 257,
        "attachments_len": 1,
    },
    # Add additional entries for other .eml fixtures if desired
}


@pytest.mark.parametrize(
    "eml_path",
    sorted(Path(__file__).parent.joinpath("eml_fixtures").glob("*.eml")),
)  # type: ignore[misc]
def test_eml_fixture_parse(eml_path: Path) -> None:
    """
    Automatically parses each .eml file in tests/eml_fixtures/.
    """
    email_obj = parse_eml(eml_path)
    # Minimal assertion: ensure it parsed successfully (e.g. has a uuid)
    assert email_obj.uuid, f"Email in {eml_path} did not parse properly"
    
    # Additional checks for all
    assert email_obj.message_id is not None
    assert email_obj.date is not None
    assert email_obj.subject is not None
    assert email_obj.from_addr is not None

    assert len(email_obj.body_text) >= 0
    assert len(email_obj.body_html) >= 0
    assert len(email_obj.attachments) >= 0

    # If the eml file has an entry in TEST_EMAIL_EXPECTATIONS, do exact field checks
    base_name = eml_path.stem
    if base_name in TEST_EMAIL_EXPECTATIONS:
        expected = TEST_EMAIL_EXPECTATIONS[base_name]
        assert email_obj.message_id == expected["message_id"]
        assert email_obj.subject == expected["subject"]
        assert email_obj.from_addr == expected["from_addr"]
        assert email_obj.date == expected["date"]
        assert len(email_obj.body_text) == expected["body_text_len"]
        assert len(email_obj.body_html) == expected["body_html_len"]
        assert len(email_obj.attachments) == expected["attachments_len"]
