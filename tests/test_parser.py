import textwrap
from pathlib import Path

from postmark.email.parser import normalise_body, parse_eml


def test_parse_plain_eml(tmp_path: Path) -> None:
    # Sample fixture EML
    eml_content = textwrap.dedent(
        """\
    From: sender@example.com
    Date: Mon, 01 Jan 2024 10:00:00 +0000
    Message-Id: <test123@example.com>
    Subject: Test Email
    Content-Type: text/plain; charset="utf-8"

    Hello world
    """
    )
    eml_file = tmp_path / "plain.eml"
    eml_file.write_text(eml_content, encoding="utf-8")

    email_obj = parse_eml(eml_file)
    assert email_obj.message_id == "<test123@example.com>"
    assert email_obj.subject == "Test Email"
    assert "Hello world" in email_obj.body_text
    assert email_obj.body_html == ""


def test_normalise_body() -> None:
    raw_body = """\
    Hello
    > quoted line
    This is normal text
    > more quoted
    """
    norm = normalise_body(raw_body)
    assert "> quoted" not in norm
    assert "Hello" in norm
    assert "This is normal text" in norm
