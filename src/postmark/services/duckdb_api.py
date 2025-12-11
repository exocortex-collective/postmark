"""REST API for DuckDB email body storage.

Provides centralized read/write access to DuckDB, solving concurrency issues
since DuckDB doesn't support multiple concurrent connections.
"""

import base64
import html
import json
import os
import re
import threading
from contextlib import asynccontextmanager
from typing import Any

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/app/data/emails.duckdb")
HOST = os.environ.get("DUCKDB_API_HOST", "0.0.0.0")
PORT = int(os.environ.get("DUCKDB_API_PORT", "8081"))


def _decode_base64_urlsafe(data: str) -> str:
    """Decode base64 URL-safe encoded data (Gmail API format)."""
    # Gmail uses URL-safe base64 without padding
    # Add padding if needed
    padded = data + "=" * (4 - len(data) % 4) if len(data) % 4 else data
    try:
        return base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _strip_html(html_content: str) -> str:
    """Strip HTML tags and decode entities to get plain text."""
    # Remove script and style elements
    text = re.sub(
        r"<script[^>]*>.*?</script>", "", html_content, flags=re.DOTALL | re.IGNORECASE
    )
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    # Replace br and p tags with newlines
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    # Remove all other HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode HTML entities
    text = html.unescape(text)
    # Collapse multiple whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _extract_body_text(parts: Any) -> str | None:
    """Extract plain text body from Gmail message parts.

    Looks for text/plain first, falls back to text/html (stripped).
    Handles nested multipart messages.
    """
    if not parts:
        return None

    # If parts is a list, search through parts
    if isinstance(parts, list):
        # First pass: look for text/plain
        for part in parts:
            mime_type = part.get("mimeType", "")
            if mime_type == "text/plain":
                body_data = part.get("body", {}).get("data")
                if body_data:
                    return _decode_base64_urlsafe(body_data)
            # Recurse into multipart
            nested_parts = part.get("parts")
            if nested_parts:
                result = _extract_body_text(nested_parts)
                if result:
                    return result

        # Second pass: fall back to text/html
        for part in parts:
            mime_type = part.get("mimeType", "")
            if mime_type == "text/html":
                body_data = part.get("body", {}).get("data")
                if body_data:
                    html_content = _decode_base64_urlsafe(body_data)
                    return _strip_html(html_content)
            # Recurse into multipart for html
            nested_parts = part.get("parts")
            if nested_parts:
                result = _extract_body_text(nested_parts)
                if result:
                    return result

    # If parts is a dict (single part), check directly
    elif isinstance(parts, dict):
        mime_type = parts.get("mimeType", "")
        body_data = parts.get("body", {}).get("data")
        if body_data:
            if mime_type == "text/plain":
                return _decode_base64_urlsafe(body_data)
            elif mime_type == "text/html":
                html_content = _decode_base64_urlsafe(body_data)
                return _strip_html(html_content)
        # Check nested parts
        nested_parts = parts.get("parts")
        if nested_parts:
            return _extract_body_text(nested_parts)

    return None


# Single connection with lock for thread safety
_conn: duckdb.DuckDBPyConnection | None = None
_lock = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get the singleton DuckDB connection."""
    global _conn
    if _conn is None:
        _conn = duckdb.connect(DUCKDB_PATH)
        # Ensure table exists
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_bodies (
                gmail_id TEXT PRIMARY KEY,
                parts JSON,
                body_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
    return _conn


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    """Initialize DuckDB connection on startup."""
    get_connection()
    yield
    # Cleanup on shutdown
    global _conn
    if _conn:
        _conn.close()
        _conn = None


app = FastAPI(
    title="DuckDB Email Bodies API",
    description="REST API for email body storage in DuckDB",
    lifespan=lifespan,
)


class BodyRequest(BaseModel):
    gmail_id: str
    parts: dict[str, Any] | list[Any] | None = None
    body_data: str | None = None


class BodyResponse(BaseModel):
    gmail_id: str
    parts: Any = None
    body_data: str | None = None
    body_text: str | None = None  # Decoded plain text (extracted from parts)


class StatusResponse(BaseModel):
    status: str


class StatsResponse(BaseModel):
    count: int


class BodiesResponse(BaseModel):
    bodies: dict[str, dict[str, Any]]


class GmailIdsResponse(BaseModel):
    gmail_ids: list[str]
    count: int


class MailTextResponse(BaseModel):
    gmail_id: str
    text: str | None  # Plain text body (rendered from HTML if no plain text part)


@app.get("/health", response_model=StatusResponse)
def health_check() -> StatusResponse:
    """Health check endpoint."""
    return StatusResponse(status="ok")


@app.get("/stats", response_model=StatsResponse)
def get_stats() -> StatsResponse:
    """Get email body count."""
    with _lock:
        conn = get_connection()
        row = conn.execute("SELECT COUNT(*) FROM email_bodies").fetchone()
    count = row[0] if row else 0
    return StatsResponse(count=count)


@app.get("/gmail_ids", response_model=GmailIdsResponse)
def get_gmail_ids(
    limit: int = Query(0, description="Max IDs to return (0 = all)"),
    offset: int = Query(0, description="Number of IDs to skip"),
) -> GmailIdsResponse:
    """Get gmail_ids stored in DuckDB (for backfill comparison).

    Supports pagination via limit/offset for large datasets.
    """
    with _lock:
        conn = get_connection()
        if limit > 0:
            rows = conn.execute(
                "SELECT gmail_id FROM email_bodies LIMIT ? OFFSET ?",
                [limit, offset],
            ).fetchall()
        else:
            rows = conn.execute("SELECT gmail_id FROM email_bodies").fetchall()
    gmail_ids = [row[0] for row in rows]
    return GmailIdsResponse(gmail_ids=gmail_ids, count=len(gmail_ids))


@app.get("/body", response_model=BodyResponse)
def get_body(
    gmail_id: str = Query(..., description="Gmail message ID")
) -> BodyResponse:
    """Get a single email body by gmail_id."""
    with _lock:
        conn = get_connection()
        result = conn.execute(
            "SELECT parts, body_data FROM email_bodies WHERE gmail_id = ?",
            [gmail_id],
        ).fetchone()

    if not result:
        raise HTTPException(
            status_code=404, detail=f"No body found for gmail_id: {gmail_id}"
        )

    # Deserialize JSON parts for cleaner API response
    parts_data = json.loads(result[0]) if result[0] else None
    # Extract decoded body text from parts
    body_text = _extract_body_text(parts_data)
    return BodyResponse(
        gmail_id=gmail_id,
        parts=parts_data,
        body_data=result[1],
        body_text=body_text,
    )


@app.get("/mail_text", response_model=MailTextResponse)
def get_mail_text(
    gmail_id: str = Query(..., description="Gmail message ID")
) -> MailTextResponse:
    """Get plain text content of an email.

    Returns decoded plain text body. If the email has only HTML, the HTML
    is rendered to plain text (tags stripped, entities decoded).
    This is the preferred endpoint for downstream text processing.
    """
    with _lock:
        conn = get_connection()
        result = conn.execute(
            "SELECT parts FROM email_bodies WHERE gmail_id = ?",
            [gmail_id],
        ).fetchone()

    if not result:
        raise HTTPException(
            status_code=404, detail=f"No body found for gmail_id: {gmail_id}"
        )

    parts_data = json.loads(result[0]) if result[0] else None
    text = _extract_body_text(parts_data)
    return MailTextResponse(gmail_id=gmail_id, text=text)


@app.get("/bodies", response_model=BodiesResponse)
def get_bodies(
    gmail_ids: str = Query(..., description="Comma-separated list of gmail IDs"),
) -> BodiesResponse:
    """Batch fetch email bodies (max 100)."""
    id_list = [g.strip() for g in gmail_ids.split(",") if g.strip()]
    if not id_list:
        raise HTTPException(status_code=400, detail="No valid gmail_ids provided")
    if len(id_list) > 100:
        raise HTTPException(status_code=400, detail="Max 100 gmail_ids per request")

    with _lock:
        conn = get_connection()
        placeholders = ",".join(["?" for _ in id_list])
        rows = conn.execute(
            f"SELECT gmail_id, parts, body_data FROM email_bodies WHERE gmail_id IN ({placeholders})",
            id_list,
        ).fetchall()

    # Deserialize JSON parts and extract body text
    bodies = {}
    for row in rows:
        parts_data = json.loads(row[1]) if row[1] else None
        bodies[row[0]] = {
            "parts": parts_data,
            "body_data": row[2],
            "body_text": _extract_body_text(parts_data),
        }
    return BodiesResponse(bodies=bodies)


@app.post("/body", response_model=StatusResponse, status_code=201)
def store_body(body: BodyRequest) -> StatusResponse:
    """Store an email body."""
    with _lock:
        conn = get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO email_bodies (gmail_id, parts, body_data)
            VALUES (?, ?, ?)
            """,
            (
                body.gmail_id,
                json.dumps(body.parts) if body.parts else None,
                body.body_data,
            ),
        )

    return StatusResponse(status="ok")


@app.post("/checkpoint", response_model=StatusResponse)
def checkpoint() -> StatusResponse:
    """Force a WAL checkpoint to flush data to main database file."""
    with _lock:
        conn = get_connection()
        conn.execute("CHECKPOINT")
    return StatusResponse(status="ok")


def main() -> None:
    """Run the DuckDB API server."""
    uvicorn.run(
        "postmark.services.duckdb_api:app",
        host=HOST,
        port=PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
