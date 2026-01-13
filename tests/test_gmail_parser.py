"""Tests for Gmail API response parsing utilities."""

import base64
from datetime import datetime, timezone

from postmark.email.gmail_parser import (
    ParsedEmail,
    decode_base64_body,
    decode_mime_header,
    extract_body_from_parts,
    extract_email_address,
    extract_email_address_only,
    extract_email_list,
    get_header,
    parse_date,
    parse_gmail_message,
    parse_references,
)


class TestDecodeMimeHeader:
    """Tests for decode_mime_header function."""

    def test_none_header(self) -> None:
        assert decode_mime_header(None) == ""

    def test_empty_header(self) -> None:
        assert decode_mime_header("") == ""

    def test_plain_ascii(self) -> None:
        assert decode_mime_header("Hello World") == "Hello World"

    def test_utf8_encoded(self) -> None:
        # =?UTF-8?B?...?= format
        result = decode_mime_header("=?UTF-8?B?SGVsbG8gV29ybGQ=?=")
        assert result == "Hello World"

    def test_multiple_encoded_parts(self) -> None:
        # Multiple encoded sections
        result = decode_mime_header("=?UTF-8?B?SGVsbG8=?= =?UTF-8?B?IFdvcmxk?=")
        assert result == "Hello World"

    def test_mixed_encoded_plain(self) -> None:
        # Mix of encoded and plain text
        result = decode_mime_header("Hello =?UTF-8?B?V29ybGQ=?=")
        assert result == "Hello World"

    def test_invalid_charset_fallback(self) -> None:
        # Invalid charset should fall back to UTF-8
        result = decode_mime_header("=?INVALID-CHARSET?B?SGVsbG8=?=")
        assert result == "Hello"


class TestExtractEmailAddress:
    """Tests for extract_email_address function."""

    def test_none_header(self) -> None:
        assert extract_email_address(None) == ("", "")

    def test_empty_header(self) -> None:
        assert extract_email_address("") == ("", "")

    def test_simple_email(self) -> None:
        addr, name = extract_email_address("user@example.com")
        assert addr == "user@example.com"
        assert name == ""

    def test_email_with_name(self) -> None:
        addr, name = extract_email_address("John Doe <john@example.com>")
        assert addr == "john@example.com"
        assert name == "John Doe"

    def test_email_with_quotes(self) -> None:
        addr, name = extract_email_address('"John Doe" <john@example.com>')
        assert addr == "john@example.com"
        assert name == "John Doe"

    def test_email_with_encoded_name(self) -> None:
        # MIME encoded display name
        addr, name = extract_email_address(
            "=?UTF-8?B?Sm9obiBEb2U=?= <john@example.com>"
        )
        assert addr == "john@example.com"
        assert name == "John Doe"


class TestExtractEmailAddressOnly:
    """Tests for extract_email_address_only function."""

    def test_simple_extraction(self) -> None:
        assert extract_email_address_only("user@example.com") == "user@example.com"

    def test_with_name(self) -> None:
        assert (
            extract_email_address_only("John Doe <john@example.com>")
            == "john@example.com"
        )

    def test_none_returns_empty(self) -> None:
        assert extract_email_address_only(None) == ""


class TestExtractEmailList:
    """Tests for extract_email_list function."""

    def test_none_header(self) -> None:
        assert extract_email_list(None) == []

    def test_empty_header(self) -> None:
        assert extract_email_list("") == []

    def test_single_email(self) -> None:
        result = extract_email_list("user@example.com")
        assert result == ["user@example.com"]

    def test_multiple_emails(self) -> None:
        result = extract_email_list("alice@example.com, bob@example.com")
        assert result == ["alice@example.com", "bob@example.com"]

    def test_emails_with_names(self) -> None:
        result = extract_email_list("Alice <alice@example.com>, Bob <bob@example.com>")
        assert result == ["alice@example.com", "bob@example.com"]

    def test_emails_with_quoted_names(self) -> None:
        # Names with commas in quotes should not split
        result = extract_email_list('"Doe, John" <john@example.com>, jane@example.com')
        assert result == ["john@example.com", "jane@example.com"]

    def test_filter_empty_addresses(self) -> None:
        # Empty addresses should be filtered out
        result = extract_email_list("alice@example.com, , bob@example.com")
        assert result == ["alice@example.com", "bob@example.com"]


class TestParseDate:
    """Tests for parse_date function."""

    def test_none_header_with_internal_date(self) -> None:
        # Fall back to internal date (milliseconds)
        internal_ms = 1700000000000  # 2023-11-14
        result = parse_date(None, internal_ms)
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.year == 2023
        assert result.month == 11

    def test_valid_rfc2822_date(self) -> None:
        result = parse_date("Mon, 13 Jan 2026 10:30:00 +0000", 0)
        assert result is not None
        assert result.year == 2026
        assert result.month == 1
        assert result.day == 13
        assert result.hour == 10
        assert result.tzinfo == timezone.utc

    def test_date_without_timezone(self) -> None:
        # Should assume UTC if no timezone
        result = parse_date("Mon, 13 Jan 2026 10:30:00", 0)
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_invalid_date_falls_back(self) -> None:
        internal_ms = 1700000000000
        result = parse_date("Invalid Date String", internal_ms)
        assert result is not None
        assert result.year == 2023

    def test_no_date_no_internal(self) -> None:
        result = parse_date(None, 0)
        assert result is None


class TestParseReferences:
    """Tests for parse_references function."""

    def test_none_header(self) -> None:
        assert parse_references(None) == []

    def test_empty_header(self) -> None:
        assert parse_references("") == []

    def test_single_message_id(self) -> None:
        result = parse_references("<message1@example.com>")
        assert result == ["<message1@example.com>"]

    def test_multiple_message_ids(self) -> None:
        result = parse_references(
            "<message1@example.com> <message2@example.com> <message3@example.com>"
        )
        assert result == [
            "<message1@example.com>",
            "<message2@example.com>",
            "<message3@example.com>",
        ]

    def test_message_ids_with_extra_whitespace(self) -> None:
        result = parse_references("  <message1@example.com>   <message2@example.com>  ")
        assert result == ["<message1@example.com>", "<message2@example.com>"]


class TestDecodeBase64Body:
    """Tests for decode_base64_body function."""

    def test_none_data(self) -> None:
        assert decode_base64_body(None) == ""

    def test_empty_data(self) -> None:
        assert decode_base64_body("") == ""

    def test_valid_base64(self) -> None:
        # "Hello World" in base64url
        result = decode_base64_body("SGVsbG8gV29ybGQ")
        assert result == "Hello World"

    def test_base64_with_padding(self) -> None:
        # Function adds padding automatically
        result = decode_base64_body("SGVsbG8")
        assert result == "Hello"

    def test_invalid_base64_returns_string(self) -> None:
        # Invalid base64 decodes with errors='replace', returns garbage but doesn't crash
        result = decode_base64_body("!!!invalid!!!")
        assert isinstance(result, str)  # Should not crash, returns string

    def test_non_utf8_decodes_with_replace(self) -> None:
        # Non-UTF8 bytes should decode with replacement character
        non_utf8 = base64.urlsafe_b64encode(b"\xff\xfe\xfd").decode("ascii")
        result = decode_base64_body(non_utf8)
        assert result == "\ufffd\ufffd\ufffd"  # Check for replacement characters


class TestExtractBodyFromParts:
    """Tests for extract_body_from_parts function."""

    def test_none_parts(self) -> None:
        assert extract_body_from_parts(None, "text/plain") == ""

    def test_empty_parts(self) -> None:
        assert extract_body_from_parts([], "text/plain") == ""

    def test_find_text_plain(self) -> None:
        parts = [
            {
                "mimeType": "text/plain",
                "body": {"data": "SGVsbG8gV29ybGQ"},  # "Hello World"
            }
        ]
        result = extract_body_from_parts(parts, "text/plain")
        assert result == "Hello World"

    def test_find_text_html(self) -> None:
        parts = [
            {
                "mimeType": "text/html",
                "body": {"data": "PGI-SGVsbG88L2I-"},  # "<b>Hello</b>"
            }
        ]
        result = extract_body_from_parts(parts, "text/html")
        assert result == "<b>Hello</b>"

    def test_multiple_parts_returns_first(self) -> None:
        parts = [
            {
                "mimeType": "text/plain",
                "body": {"data": "Rmlyc3Q"},  # "First"
            },
            {
                "mimeType": "text/plain",
                "body": {"data": "U2Vjb25k"},  # "Second"
            },
        ]
        result = extract_body_from_parts(parts, "text/plain")
        assert result == "First"

    def test_nested_parts_recursive(self) -> None:
        # Multipart with nested parts
        parts = [
            {
                "mimeType": "multipart/alternative",
                "parts": [
                    {
                        "mimeType": "text/plain",
                        "body": {"data": "TmVzdGVk"},  # "Nested"
                    }
                ],
            }
        ]
        result = extract_body_from_parts(parts, "text/plain")
        assert result == "Nested"

    def test_missing_mime_type_in_part(self) -> None:
        parts = [{"body": {"data": "SGVsbG8"}}]
        result = extract_body_from_parts(parts, "text/plain")
        assert result == ""

    def test_missing_body_in_part(self) -> None:
        parts = [{"mimeType": "text/plain"}]
        result = extract_body_from_parts(parts, "text/plain")
        assert result == ""


class TestGetHeader:
    """Tests for get_header function."""

    def test_empty_headers(self) -> None:
        assert get_header([], "From") is None

    def test_find_existing_header(self) -> None:
        headers = [
            {"name": "From", "value": "user@example.com"},
            {"name": "To", "value": "recipient@example.com"},
        ]
        assert get_header(headers, "From") == "user@example.com"

    def test_case_insensitive(self) -> None:
        headers = [{"name": "From", "value": "user@example.com"}]
        assert get_header(headers, "from") == "user@example.com"
        assert get_header(headers, "FROM") == "user@example.com"

    def test_missing_header(self) -> None:
        headers = [{"name": "From", "value": "user@example.com"}]
        assert get_header(headers, "To") is None

    def test_returns_first_match(self) -> None:
        # If multiple headers with same name (shouldn't happen but handle it)
        headers = [
            {"name": "Received", "value": "first"},
            {"name": "Received", "value": "second"},
        ]
        assert get_header(headers, "Received") == "first"


class TestParseGmailMessage:
    """Tests for parse_gmail_message function."""

    def test_basic_email(self) -> None:
        raw = {
            "gmail_id": "abc123",
            "headers": [
                {"name": "From", "value": "Alice <alice@example.com>"},
                {"name": "To", "value": "bob@example.com"},
                {"name": "Subject", "value": "Test Subject"},
                {"name": "Date", "value": "Mon, 13 Jan 2026 10:00:00 +0000"},
                {"name": "Message-ID", "value": "<msg1@example.com>"},
            ],
            "internal_date": 1736762400000,
            "mime_type": "text/plain",
        }
        body_data = {"body_data": "SGVsbG8gV29ybGQ"}  # "Hello World"

        result = parse_gmail_message(raw, body_data)

        assert isinstance(result, ParsedEmail)
        assert result.gmail_id == "abc123"
        assert result.from_addr == "alice@example.com"
        assert result.from_name == "Alice"
        assert result.to_addrs == ["bob@example.com"]
        assert result.subject == "Test Subject"
        assert result.body_text == "Hello World"
        assert result.body_html == ""
        assert result.message_id == "<msg1@example.com>"

    def test_multipart_email(self) -> None:
        raw = {
            "gmail_id": "xyz789",
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "recipient@example.com"},
                {"name": "Subject", "value": "Multipart Test"},
                {"name": "Date", "value": "Mon, 13 Jan 2026 10:00:00 +0000"},
            ],
            "internal_date": 1736762400000,
            "mime_type": "multipart/alternative",
        }
        body_data = {
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": "UGxhaW4gdGV4dA"},  # "Plain text"
                },
                {
                    "mimeType": "text/html",
                    "body": {"data": "PGI-SFRNTDwvYj4"},  # "<b>HTML</b>"
                },
            ]
        }

        result = parse_gmail_message(raw, body_data)

        assert result.body_text == "Plain text"
        assert result.body_html == "<b>HTML</b>"

    def test_email_with_cc_bcc(self) -> None:
        raw = {
            "gmail_id": "test123",
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "to1@example.com, to2@example.com"},
                {"name": "Cc", "value": "cc1@example.com"},
                {"name": "Bcc", "value": "bcc1@example.com"},
                {"name": "Subject", "value": "Test"},
                {"name": "Date", "value": "Mon, 13 Jan 2026 10:00:00 +0000"},
            ],
            "internal_date": 1736762400000,
            "mime_type": "text/plain",
        }
        body_data = {}

        result = parse_gmail_message(raw, body_data)

        assert result.to_addrs == ["to1@example.com", "to2@example.com"]
        assert result.cc_addrs == ["cc1@example.com"]
        assert result.bcc_addrs == ["bcc1@example.com"]

    def test_email_with_references(self) -> None:
        raw = {
            "gmail_id": "thread123",
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "recipient@example.com"},
                {"name": "Subject", "value": "Re: Original"},
                {"name": "Date", "value": "Mon, 13 Jan 2026 10:00:00 +0000"},
                {"name": "In-Reply-To", "value": "<original@example.com>"},
                {
                    "name": "References",
                    "value": "<msg1@example.com> <msg2@example.com>",
                },
            ],
            "internal_date": 1736762400000,
            "mime_type": "text/plain",
        }
        body_data = {}

        result = parse_gmail_message(raw, body_data)

        assert result.in_reply_to == "<original@example.com>"
        assert result.references == ["<msg1@example.com>", "<msg2@example.com>"]

    def test_undisclosed_recipients_fallback(self) -> None:
        # When To: is "undisclosed-recipients", fall back to Delivered-To
        raw = {
            "gmail_id": "bcc123",
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "undisclosed-recipients:;"},
                {"name": "Delivered-To", "value": "actual@example.com"},
                {"name": "Subject", "value": "BCC Test"},
                {"name": "Date", "value": "Mon, 13 Jan 2026 10:00:00 +0000"},
            ],
            "internal_date": 1736762400000,
            "mime_type": "text/plain",
        }
        body_data = {}

        result = parse_gmail_message(raw, body_data)

        assert result.to_addrs == ["actual@example.com"]

    def test_html_body_without_parts(self) -> None:
        # Single-part HTML email (no parts array)
        raw = {
            "gmail_id": "html123",
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "recipient@example.com"},
                {"name": "Subject", "value": "HTML Email"},
                {"name": "Date", "value": "Mon, 13 Jan 2026 10:00:00 +0000"},
            ],
            "internal_date": 1736762400000,
            "mime_type": "text/html",
        }
        body_data = {"body_data": "PGh0bWw-VGVzdDwvaHRtbD4"}  # "<html>Test</html>"

        result = parse_gmail_message(raw, body_data)

        assert result.body_text == ""
        assert result.body_html == "<html>Test</html>"

    def test_missing_optional_fields(self) -> None:
        # Minimal email with only required fields
        raw = {
            "gmail_id": "minimal123",
            "headers": [
                {"name": "From", "value": "sender@example.com"},
            ],
            "internal_date": 1736762400000,
            "mime_type": "text/plain",
        }
        body_data = {}

        result = parse_gmail_message(raw, body_data)

        assert result.gmail_id == "minimal123"
        assert result.from_addr == "sender@example.com"
        assert result.to_addrs == []
        assert result.cc_addrs == []
        assert result.bcc_addrs == []
        assert result.subject == ""
        assert result.message_id == ""
        assert result.in_reply_to == ""
        assert result.references == []
        assert result.body_text == ""
        assert result.body_html == ""
