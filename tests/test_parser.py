import textwrap
from pathlib import Path

from postmark.email.parser import normalise_body, parse_email, parse_email_date


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

    email_obj = parse_email(eml_file)
    assert email_obj.message_id == "<test123@example.com>"
    assert email_obj.subject == "Test Email"
    assert "Hello world" in email_obj.body_text
    assert email_obj.body_html == ""


def test_normalise_body() -> None:
    raw_body = """\
    <div><b>Hello HTML</b></div>
    Hello
    > quoted line
    This is normal text
    > more quoted
    """
    norm = normalise_body(raw_body)
    assert "HTML" in norm
    assert "> quoted" not in norm
    assert "Hello" in norm
    assert "This is normal text" in norm


def test_strip_non_text_attachments(tmp_path: Path) -> None:
    eml_content = (
        "From: sender@example.com\n"
        "Date: Mon, 01 Jan 2024 10:00:00 +0000\n"
        "Message-Id: <test123@example.com>\n"
        "Subject: Test Email\n"
        "MIME-Version: 1.0\n"
        "Content-Type: multipart/mixed; boundary=BOUNDARY\n"
        "\n"
        "--BOUNDARY\n"
        "Content-Type: text/plain; charset=us-ascii\n"
        "\n"
        "This is the body.\n"
        "--BOUNDARY\n"
        "Content-Type: application/octet-stream\n"
        'Content-Disposition: attachment; filename="file.bin"\n'
        "\n"
        "BINARYDATA\n"
        "--BOUNDARY--\n"
    )
    eml_file = tmp_path / "with_attach.eml"
    eml_file.write_text(eml_content, encoding="utf-8")
    email_obj = parse_email(eml_file)
    # The raw_msg should not contain the binary attachment filename
    assert b"file.bin" not in email_obj.raw_msg
    # The raw_msg should contain the text body
    assert b"This is the body." in email_obj.raw_msg


def test_parse_email_date_none() -> None:
    """Test parse_email_date with None input."""
    result = parse_email_date(None)
    assert result is None


def test_parse_email_date_invalid() -> None:
    """Test parse_email_date with invalid date string."""
    result = parse_email_date("not a valid date")
    assert result is None


def test_parse_email_with_html_body(tmp_path: Path) -> None:
    """Test parsing email with HTML body."""
    eml_content = textwrap.dedent(
        """\
        From: sender@example.com
        Date: Mon, 01 Jan 2024 10:00:00 +0000
        Message-Id: <html123@example.com>
        Subject: HTML Email
        Content-Type: text/html; charset="utf-8"

        <html><body><p>Hello HTML</p></body></html>
        """
    )
    eml_file = tmp_path / "html.eml"
    eml_file.write_text(eml_content, encoding="utf-8")

    email_obj = parse_email(eml_file)
    assert email_obj.message_id == "<html123@example.com>"
    assert "<p>Hello HTML</p>" in email_obj.body_html
    assert email_obj.body_text == ""


def test_parse_email_multipart_with_html(tmp_path: Path) -> None:
    """Test parsing multipart email with both text and HTML."""
    eml_content = (
        "From: sender@example.com\n"
        "Date: Mon, 01 Jan 2024 10:00:00 +0000\n"
        "Message-Id: <multi123@example.com>\n"
        "Subject: Multipart Email\n"
        "MIME-Version: 1.0\n"
        "Content-Type: multipart/alternative; boundary=BOUNDARY\n"
        "\n"
        "--BOUNDARY\n"
        "Content-Type: text/plain; charset=utf-8\n"
        "\n"
        "Plain text version\n"
        "--BOUNDARY\n"
        "Content-Type: text/html; charset=utf-8\n"
        "\n"
        "<html><body><b>HTML version</b></body></html>\n"
        "--BOUNDARY--\n"
    )
    eml_file = tmp_path / "multipart.eml"
    eml_file.write_text(eml_content, encoding="utf-8")

    email_obj = parse_email(eml_file)
    assert "Plain text version" in email_obj.body_text
    assert "<b>HTML version</b>" in email_obj.body_html


def test_parse_email_missing_date(tmp_path: Path) -> None:
    """Test parsing email without Date header."""
    eml_content = textwrap.dedent(
        """\
        From: sender@example.com
        Message-Id: <nodate@example.com>
        Subject: No Date
        Content-Type: text/plain; charset="utf-8"

        Body without date
        """
    )
    eml_file = tmp_path / "nodate.eml"
    eml_file.write_text(eml_content, encoding="utf-8")

    email_obj = parse_email(eml_file)
    assert email_obj.date is None
    assert email_obj.message_id == "<nodate@example.com>"
