from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from postmark.email.parser import parse_email

# Dictionary of per-file test expectations.
# "key" should match the base name (stem) of the .eml file
# "value" is a dict of expected fields.
# For body_text, body_html, attachments, store expected lengths (or set them to None to skip).
TEST_EMAIL_EXPECTATIONS = {
    "allen-p.69": {
        "message_id": "<31795707.1075855686778.JavaMail.evans@thyme>",
        "subject": "",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 14, 9, 8, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 1337,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["pallen70@hotmail.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.70": {
        "message_id": "<2578471.1075855686799.JavaMail.evans@thyme>",
        "subject": "New Employee on 32",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 14, 6, 33, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 615,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["ina.rangel@enron.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.71": {
        "message_id": "<17606686.1075855686820.JavaMail.evans@thyme>",
        "subject": "Re: Resumes",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 13, 5, 26, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 78,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["cbpres@austin.rr.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.72": {
        "message_id": "<32606382.1075855686844.JavaMail.evans@thyme>",
        "subject": "Re: West Gas 2001 Plan",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 13, 5, 25, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 958,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["faith.killen@enron.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.73": {
        "message_id": "<4495124.1075855686866.JavaMail.evans@thyme>",
        "subject": "ALLEN DURANGO HOTEL ------- 48 HR CANCEL",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 3, 7, 43, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 1595,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["pallen70@hotmail.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.74": {
        "message_id": "<18528711.1075855686888.JavaMail.evans@thyme>",
        "subject": "New Generation as of Oct 24th",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 3, 5, 57, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 1518,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": [
            "mike.grigsby@enron.com",
            "keith.holst@enron.com",
            "frank.ermis@enron.com",
        ],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.75": {
        "message_id": "<26920113.1075855686909.JavaMail.evans@thyme>",
        "subject": "",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 2, 8, 12, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 504,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["pallen70@hotmail.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.76": {
        "message_id": "<31937997.1075855686930.JavaMail.evans@thyme>",
        "subject": "Re: Resumes",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 2, 5, 26, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 119,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["cbpres@austin.rr.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    "allen-p.77": {
        "message_id": "<11681568.1075855686951.JavaMail.evans@thyme>",
        "subject": "Resumes",
        "from_addr": "phillip.allen@enron.com",
        "date": datetime(
            2000, 11, 2, 2, 36, tzinfo=timezone(timedelta(days=-1, seconds=57600))
        ),
        "body_text_len": 479,
        "body_html_len": 0,
        "attachments_len": 0,
        "to_addrs": ["colin.tonks@enron.com"],
        "cc_addrs": [],
        "bcc_addrs": [],
    },
    # Add additional entries for other .eml fixtures if desired
}


@pytest.mark.parametrize(
    "eml_path",
    sorted(Path(__file__).parent.joinpath("eml_fixtures").glob("*.eml")),
)
def test_eml_fixture_parse(eml_path: Path) -> None:
    """
    Automatically parses each .eml file in tests/eml_fixtures/.
    """
    email_obj = parse_email(eml_path)
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
        assert email_obj.to_addrs == expected["to_addrs"]
        assert email_obj.cc_addrs == expected["cc_addrs"]
        assert email_obj.bcc_addrs == expected["bcc_addrs"]
    else:
        raise ValueError(f"No expectations for {base_name}")
