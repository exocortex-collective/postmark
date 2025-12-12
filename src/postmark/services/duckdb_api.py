"""REST API for DuckDB email body storage.

Provides centralized read/write access to DuckDB, solving concurrency issues
since DuckDB doesn't support multiple concurrent connections.
"""

import base64
import json
import os
import threading
from contextlib import asynccontextmanager
from typing import Any

import duckdb
import html2text
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/app/data/emails.duckdb")
HOST = os.environ.get("DUCKDB_API_HOST", "0.0.0.0")
PORT = int(os.environ.get("DUCKDB_API_PORT", "8081"))

# Configure html2text for clean plain text output
_html2text = html2text.HTML2Text()
_html2text.ignore_links = False  # Keep link URLs inline
_html2text.ignore_images = True
_html2text.ignore_emphasis = True  # Don't add * for bold/italic
_html2text.body_width = 0  # Don't wrap lines


def _decode_base64_urlsafe(data: str) -> str:
    """Decode base64 URL-safe encoded data (Gmail API format)."""
    # Gmail uses URL-safe base64 without padding
    # Add padding if needed
    padded = data + "=" * (4 - len(data) % 4) if len(data) % 4 else data
    try:
        return base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
    except (ValueError, UnicodeDecodeError):
        return ""


def _html_to_text(html_content: str) -> str:
    """Convert HTML to plain text using html2text."""
    result: str = _html2text.handle(html_content)
    return result.strip()


def _extract_body_text(parts: Any) -> str | None:
    """Extract plain text body from Gmail message parts.

    Looks for text/plain first, falls back to text/html (stripped).
    Handles nested multipart messages using iterative DFS.
    """
    if not parts:
        return None

    plain_body_data = None
    html_body_data = None

    # Use a stack for iterative DFS traversal
    stack: list[Any] = []
    if isinstance(parts, list):
        stack.extend(reversed(parts))
    elif isinstance(parts, dict):
        stack.append(parts)

    while stack:
        part = stack.pop()

        # Add nested parts to stack for DFS
        nested_parts = part.get("parts")
        if nested_parts:
            stack.extend(reversed(nested_parts))

        mime_type = part.get("mimeType", "")
        if mime_type == "text/plain":
            body_data = part.get("body", {}).get("data")
            if body_data:
                plain_body_data = body_data
                break  # Found plain text, preferred format - stop search

        elif mime_type == "text/html" and not html_body_data:
            # Store first HTML part as fallback
            body_data = part.get("body", {}).get("data")
            if body_data:
                html_body_data = body_data

    if plain_body_data:
        return _decode_base64_urlsafe(plain_body_data)

    if html_body_data:
        html_content = _decode_base64_urlsafe(html_body_data)
        return _html_to_text(html_content)

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


class BodyDetails(BaseModel):
    """Details for a single email body in batch responses."""

    parts: Any = None
    body_data: str | None = None
    body_text: str | None = None


class BodiesResponse(BaseModel):
    bodies: dict[str, BodyDetails]


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
    bodies: dict[str, BodyDetails] = {}
    for row in rows:
        parts_data = json.loads(row[1]) if row[1] else None
        bodies[row[0]] = BodyDetails(
            parts=parts_data,
            body_data=row[2],
            body_text=_extract_body_text(parts_data),
        )
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
