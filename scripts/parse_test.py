#!/usr/bin/env python3
"""Test email parsing against live data - iterate until all emails parse cleanly."""

import json
import os
import sys
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from postmark.email.gmail_parser import parse_gmail_message

# Configuration - all required from environment (use .env.template as reference)
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
DUCKDB_API_URL = os.environ.get(
    "DUCKDB_API_URL"
)  # Set for remote API, or use DUCKDB_PATH for local
DUCKDB_LOCAL_PATH = os.environ.get("DUCKDB_PATH", "")


@dataclass
class TestParsedEmail:
    """Test wrapper with thread_id and simplified fields for validation."""

    gmail_id: str
    thread_id: str
    from_addr: str
    to_addr: str  # First to address for simple validation
    cc: list[str]
    bcc: list[str]
    subject: str
    date: datetime | None
    body_text: str
    body_html: str


def parse_email(row: dict[str, Any], body_data: dict[str, Any]) -> TestParsedEmail:
    """Parse using production code and wrap for testing."""
    # Use the production parser
    parsed = parse_gmail_message(row, body_data)

    # Wrap in test structure with thread_id
    return TestParsedEmail(
        gmail_id=parsed.gmail_id,
        thread_id=row["thread_id"],
        from_addr=parsed.from_addr,
        to_addr=parsed.to_addrs[0] if parsed.to_addrs else "",
        cc=parsed.cc_addrs,
        bcc=parsed.bcc_addrs,
        subject=parsed.subject,
        date=parsed.date_header,
        body_text=parsed.body_text,
        body_html=parsed.body_html,
    )


def validate_email(parsed: TestParsedEmail, internal_date_ms: int) -> list[str]:
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

    # Note: missing subject is valid - some emails have no subject

    if not parsed.date:
        issues.append("MISSING date")
    else:
        # Check date is in reasonable range (2020 to now + 5 minute grace period)
        min_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        max_date = datetime.now(timezone.utc) + timedelta(minutes=5)
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


def get_body_data(gmail_id: str, duck_conn: Any = None) -> dict[str, Any]:
    """Fetch body data from DuckDB (via API or local connection)."""
    if DUCKDB_API_URL:
        # Use REST API
        url = f"{DUCKDB_API_URL}/body?gmail_id={gmail_id}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                return {"parts": data.get("parts"), "body_data": data.get("body_data")}
        except (urllib.error.URLError, TimeoutError):
            return {"parts": None, "body_data": None}
        except json.JSONDecodeError:
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
    # Validate required env vars
    if not POSTGRES_PASSWORD:
        print("Error: POSTGRES_PASSWORD environment variable is required")
        print("Usage: POSTGRES_PASSWORD=xxx python scripts/parse_test.py")
        return

    # Connect to databases
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    duck_conn = None
    if not DUCKDB_API_URL:
        import duckdb

        duck_conn = duckdb.connect(DUCKDB_LOCAL_PATH, read_only=True)

    try:
        _run_tests(pg_conn, duck_conn)
    finally:
        pg_conn.close()
        if duck_conn:
            duck_conn.close()


def _run_tests(pg_conn: Any, duck_conn: Any) -> None:
    """Run the actual parse tests."""
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

    # Track issues and stats in a single pass
    total_issues = 0
    emails_with_issues = 0
    has_text_only = 0
    has_html_only = 0
    has_both = 0
    has_cc = 0
    from_domains: dict[str, int] = {}
    dates: list[datetime] = []

    for row in rows:
        body_data = get_body_data(row["gmail_id"], duck_conn)

        try:
            parsed = parse_email(row, body_data)
            issues = validate_email(parsed, row["internal_date"])

            # Collect stats during validation pass
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
            traceback.print_exc()
            print()

    print(f"\n{'='*60}")
    print(
        f"Summary: {len(rows)} emails, {emails_with_issues} with issues, {total_issues} total issues"
    )

    print(f"\n{'='*60}")
    print("Statistics:\n")
    print(
        f"Body types: {has_text_only} text-only, {has_html_only} html-only, {has_both} both"
    )
    print(f"Emails with CC: {has_cc}")
    if dates:
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
        body_data = get_body_data(row["gmail_id"], duck_conn)
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
