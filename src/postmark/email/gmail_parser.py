"""Gmail API response parsing utilities.

This module provides functions for parsing email data from Gmail API responses,
including MIME header decoding, address extraction, and body content parsing.
"""

import base64
import binascii
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any


@dataclass
class ParsedEmail:
    """Structured email data parsed from Gmail API response."""

    gmail_id: str
    from_addr: str
    from_name: str
    to_addrs: list[str]
    cc_addrs: list[str]
    bcc_addrs: list[str]
    subject: str
    date_header: datetime | None
    message_id: str
    in_reply_to: str
    references: list[str]
    body_text: str
    body_html: str


def decode_mime_header(header_value: str | None) -> str:
    """Decode RFC 2047 encoded header."""
    if not header_value:
        return ""

    decoded_parts = []
    for part, charset in decode_header(header_value):
        if isinstance(part, bytes):
            charset = charset or "utf-8"
            try:
                decoded_parts.append(part.decode(charset, errors="replace"))
            except (LookupError, UnicodeDecodeError):
                decoded_parts.append(part.decode("utf-8", errors="replace"))
        else:
            decoded_parts.append(part)
    return "".join(decoded_parts)


def extract_email_address(header_value: str | None) -> tuple[str, str]:
    """Extract email address and display name from header.

    Returns (address, name) tuple. Returns empty string for address if none found.
    """
    if not header_value:
        return ("", "")
    decoded = decode_mime_header(header_value)
    name, addr = parseaddr(decoded)
    return (addr or "", name)


def extract_email_address_only(header_value: str | None) -> str:
    """Extract just the email address from header (for simple cases)."""
    addr, _ = extract_email_address(header_value)
    return addr


def extract_email_list(header_value: str | None) -> list[str]:
    """Extract list of email addresses from comma-separated header."""
    if not header_value:
        return []
    decoded = decode_mime_header(header_value)
    # Split on comma, but be careful with quoted names containing commas
    addresses = []
    for part in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', decoded):
        part = part.strip()
        if part:
            _, addr = parseaddr(part)
            addresses.append(addr or part)
    return [a for a in addresses if a]


def parse_date(header_value: str | None, internal_date_ms: int) -> datetime | None:
    """Parse date from header, falling back to internal_date."""
    if header_value:
        try:
            dt = parsedate_to_datetime(header_value)
            # Ensure timezone awareness
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            pass

    # Fall back to Gmail's internal date
    if internal_date_ms:
        return datetime.fromtimestamp(internal_date_ms / 1000, tz=timezone.utc)
    return None


def parse_references(header_value: str | None) -> list[str]:
    """Parse References header into list of message IDs."""
    if not header_value:
        return []
    # Message-IDs are enclosed in angle brackets
    return re.findall(r"<[^>]+>", header_value)


def decode_base64_body(data: str | None) -> str:
    """Decode base64url encoded body data."""
    if not data:
        return ""
    try:
        # Gmail uses URL-safe base64
        decoded = base64.urlsafe_b64decode(data + "==")
        return decoded.decode("utf-8", errors="replace")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        return ""


def extract_body_from_parts(parts: list[dict[str, Any]] | None, mime_type: str) -> str:
    """Recursively extract body content from MIME parts."""
    if not parts:
        return ""

    for part in parts:
        part_mime = part.get("mimeType", "")

        if part_mime == mime_type:
            body = part.get("body", {})
            data = body.get("data")
            if data:
                return decode_base64_body(data)

        # Recurse into nested parts
        nested = part.get("parts")
        if nested:
            result = extract_body_from_parts(nested, mime_type)
            if result:
                return result

    return ""


def get_header(headers: list[dict[str, str]], name: str) -> str | None:
    """Get header value by name (case-insensitive)."""
    name_lower = name.lower()
    for h in headers:
        if h.get("name", "").lower() == name_lower:
            return h.get("value")
    return None


def parse_gmail_message(raw: dict[str, Any], body_data: dict[str, Any]) -> ParsedEmail:
    """Parse a raw Gmail API response into structured fields.

    Args:
        raw: Row from emails_raw table with gmail_id, headers, internal_date, mime_type
        body_data: Body data from DuckDB with parts and body_data fields

    Returns:
        ParsedEmail with all extracted fields
    """
    headers = raw["headers"]  # Already JSON from Postgres

    from_addr, from_name = extract_email_address(get_header(headers, "From"))

    to_header = get_header(headers, "To")
    # Fall back to Delivered-To for BCC'd emails or undisclosed-recipients
    if not to_header or "undisclosed-recipients" in (to_header or "").lower():
        to_header = get_header(headers, "Delivered-To")
    to_addrs = extract_email_list(to_header)

    cc_addrs = extract_email_list(get_header(headers, "Cc"))
    bcc_addrs = extract_email_list(get_header(headers, "Bcc"))
    subject = decode_mime_header(get_header(headers, "Subject"))
    date = parse_date(get_header(headers, "Date"), raw["internal_date"])
    message_id = get_header(headers, "Message-ID") or ""
    in_reply_to = get_header(headers, "In-Reply-To") or ""
    references = parse_references(get_header(headers, "References"))

    # Extract body from DuckDB data (both API and local return parsed JSON)
    parts_list = body_data.get("parts")

    body_text = ""
    body_html = ""

    # Try to get body from parts first (multipart emails)
    if parts_list:
        body_text = extract_body_from_parts(parts_list, "text/plain")
        body_html = extract_body_from_parts(parts_list, "text/html")

    # Fall back to direct body_data if no parts (single-part emails)
    if not body_text and not body_html:
        direct_body = body_data.get("body_data")
        if direct_body:
            decoded = decode_base64_body(direct_body)
            # Use mime_type from raw email to determine body type
            mime_type = raw.get("mime_type", "text/plain")
            if mime_type == "text/html":
                body_html = decoded
            else:
                body_text = decoded

    return ParsedEmail(
        gmail_id=raw["gmail_id"],
        from_addr=from_addr,
        from_name=from_name,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        bcc_addrs=bcc_addrs,
        subject=subject,
        date_header=date,
        message_id=message_id,
        in_reply_to=in_reply_to,
        references=references,
        body_text=body_text,
        body_html=body_html,
    )
