#!/usr/bin/env python3
"""Test email parsing against live data - iterate until all emails parse cleanly."""

import base64
import json
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any

import psycopg2

# Connect to Postgres
pg_conn = psycopg2.connect(
    host="10.5.2.21",
    dbname="cortex",
    user="cortex",
    password="cortex",
)

# DuckDB API endpoint (set to None to use local file)
DUCKDB_API_URL = os.environ.get("DUCKDB_API_URL")  # e.g., "http://10.5.2.21:8081"
DUCKDB_LOCAL_PATH = os.environ.get("DUCKDB_PATH", "/tmp/cortex-data/emails.duckdb")

# Local DuckDB connection (if not using API)
duck_conn = None
if not DUCKDB_API_URL:
    import duckdb

    duck_conn = duckdb.connect(DUCKDB_LOCAL_PATH, read_only=True)


@dataclass
class ParsedEmail:
    gmail_id: str
    thread_id: str
    from_addr: str
    to_addr: str
    cc: list[str]
    bcc: str
    subject: str
    date: datetime | None
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


def extract_email_address(header_value: str | None) -> str:
    """Extract clean email address from header like 'Name <email@example.com>'."""
    if not header_value:
        return ""
    decoded = decode_mime_header(header_value)
    name, addr = parseaddr(decoded)
    return addr or decoded


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
            return parsedate_to_datetime(header_value)
        except (ValueError, TypeError):
            pass

    # Fall back to Gmail's internal date
    if internal_date_ms:
        return datetime.fromtimestamp(internal_date_ms / 1000, tz=timezone.utc)
    return None


def decode_base64_body(data: str | None) -> str:
    """Decode base64url encoded body data."""
    if not data:
        return ""
    try:
        # Gmail uses URL-safe base64
        decoded = base64.urlsafe_b64decode(data + "==")
        return decoded.decode("utf-8", errors="replace")
    except Exception:
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


def parse_email(row: dict[str, Any], body_data: dict[str, Any]) -> ParsedEmail:
    """Parse a raw email into structured fields."""
    headers = row["headers"]  # Already JSON

    from_addr = extract_email_address(get_header(headers, "From"))
    to_addr = extract_email_address(get_header(headers, "To"))
    cc = extract_email_list(get_header(headers, "Cc"))
    bcc = extract_email_address(get_header(headers, "Bcc"))
    subject = decode_mime_header(get_header(headers, "Subject"))
    date = parse_date(get_header(headers, "Date"), row["internal_date"])

    # Extract body from DuckDB data
    parts = body_data.get("parts")
    if parts:
        parts_list = json.loads(parts) if isinstance(parts, str) else parts
    else:
        parts_list = None

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
            mime_type = row.get("mime_type", "text/plain")
            if mime_type == "text/html":
                body_html = decoded
            else:
                body_text = decoded

    return ParsedEmail(
        gmail_id=row["gmail_id"],
        thread_id=row["thread_id"],
        from_addr=from_addr,
        to_addr=to_addr,
        cc=cc,
        bcc=bcc,
        subject=subject,
        date=date,
        body_text=body_text,
        body_html=body_html,
    )


def validate_email(parsed: ParsedEmail, internal_date_ms: int) -> list[str]:
    """Check for issues with parsed email. Returns list of problems."""
    issues = []

    if not parsed.from_addr:
        issues.append("MISSING from_addr")
    elif "@" not in parsed.from_addr:
        issues.append(f"INVALID from_addr: {parsed.from_addr!r}")

    if not parsed.to_addr:
        issues.append("MISSING to_addr")
    elif "@" not in parsed.to_addr:
        issues.append(f"INVALID to_addr: {parsed.to_addr!r}")

    if not parsed.subject:
        issues.append("MISSING subject")

    if not parsed.date:
        issues.append("MISSING date")
    else:
        # Check date is in reasonable range (2020 to now + 1 day)
        min_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        max_date = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
        if parsed.date < min_date:
            issues.append(f"DATE too old: {parsed.date}")
        elif parsed.date > max_date:
            issues.append(f"DATE in future: {parsed.date}")

        # Compare with Gmail's internal_date
        if internal_date_ms:
            gmail_date = datetime.fromtimestamp(
                internal_date_ms / 1000, tz=timezone.utc
            )
            diff = abs((parsed.date - gmail_date).total_seconds())
            if diff > 86400 * 7:  # More than 7 days difference
                issues.append(
                    f"DATE mismatch: parsed={parsed.date}, gmail={gmail_date}"
                )

    if not parsed.body_text and not parsed.body_html:
        issues.append("MISSING body (both text and html)")

    return issues


def get_body_data(gmail_id: str) -> dict[str, Any]:
    """Fetch body data from DuckDB (via API or local connection)."""
    if DUCKDB_API_URL:
        # Use REST API
        url = f"{DUCKDB_API_URL}/body?gmail_id={gmail_id}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                return {"parts": data.get("parts"), "body_data": data.get("body_data")}
        except Exception:
            return {"parts": None, "body_data": None}
    else:
        # Use local DuckDB connection
        if duck_conn is None:
            return {"parts": None, "body_data": None}
        result = duck_conn.execute(
            "SELECT parts, body_data FROM email_bodies WHERE gmail_id = ?",
            [gmail_id],
        ).fetchone()
        if result:
            return {"parts": result[0], "body_data": result[1]}
        return {"parts": None, "body_data": None}


def main() -> None:
    # Fetch all raw emails
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT gmail_id, thread_id, headers, internal_date, snippet, mime_type
        FROM emails_raw
        ORDER BY internal_date DESC
    """
    )

    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    print(f"Processing {len(rows)} emails...\n")

    total_issues = 0
    emails_with_issues = 0

    for row in rows:
        body_data = get_body_data(row["gmail_id"])

        try:
            parsed = parse_email(row, body_data)
            issues = validate_email(parsed, row["internal_date"])

            if issues:
                emails_with_issues += 1
                total_issues += len(issues)
                print(
                    f"❌ {parsed.gmail_id[:12]}... | {parsed.subject[:50] if parsed.subject else '(no subject)'}"
                )
                for issue in issues:
                    print(f"   └─ {issue}")
                print()
            else:
                print(
                    f"✓ {parsed.gmail_id[:12]}... | {parsed.subject[:50] if parsed.subject else '(no subject)'}"
                )

        except Exception as e:
            emails_with_issues += 1
            total_issues += 1
            print(f"💥 {row['gmail_id'][:12]}... | PARSE ERROR: {e}")
            import traceback

            traceback.print_exc()
            print()

    print(f"\n{'='*60}")
    print(
        f"Summary: {len(rows)} emails, {emails_with_issues} with issues, {total_issues} total issues"
    )

    # Collect stats
    has_text_only = 0
    has_html_only = 0
    has_both = 0
    has_cc = 0
    from_domains: dict[str, int] = {}
    dates: list[datetime] = []

    for row in rows:
        body_data = get_body_data(row["gmail_id"])
        parsed = parse_email(row, body_data)

        if parsed.body_text and parsed.body_html:
            has_both += 1
        elif parsed.body_text:
            has_text_only += 1
        elif parsed.body_html:
            has_html_only += 1

        if parsed.cc:
            has_cc += 1

        if parsed.from_addr and "@" in parsed.from_addr:
            domain = parsed.from_addr.split("@")[1].lower()
            from_domains[domain] = from_domains.get(domain, 0) + 1

        if parsed.date:
            dates.append(parsed.date)

    print(f"\n{'='*60}")
    print("Statistics:\n")
    print(
        f"Body types: {has_text_only} text-only, {has_html_only} html-only, {has_both} both"
    )
    print(f"Emails with CC: {has_cc}")
    print(f"Date range: {min(dates)} to {max(dates)}")
    print(f"\nTop sender domains:")
    for domain, count in sorted(from_domains.items(), key=lambda x: -x[1])[:10]:
        print(f"  {domain}: {count}")

    # Show a few sample parsed emails for manual inspection
    print(f"\n{'='*60}")
    print("Sample parsed emails:\n")

    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT gmail_id, thread_id, headers, internal_date, snippet, mime_type
        FROM emails_raw
        ORDER BY internal_date DESC
        LIMIT 3
    """
    )
    columns = [desc[0] for desc in cur.description]
    sample_rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    for row in sample_rows:
        body_data = get_body_data(row["gmail_id"])
        parsed = parse_email(row, body_data)
        print(f"Gmail ID: {parsed.gmail_id}")
        print(f"Thread ID: {parsed.thread_id}")
        print(f"From: {parsed.from_addr}")
        print(f"To: {parsed.to_addr}")
        print(f"CC: {parsed.cc}")
        print(f"Subject: {parsed.subject}")
        print(f"Date: {parsed.date}")
        print(f"Body text length: {len(parsed.body_text)} chars")
        print(f"Body html length: {len(parsed.body_html)} chars")
        preview = parsed.body_text[:200] if parsed.body_text else parsed.body_html[:200]
        print(f"Body preview: {preview}...")
        print("-" * 40)


if __name__ == "__main__":
    main()
