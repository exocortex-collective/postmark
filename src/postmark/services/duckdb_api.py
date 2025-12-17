"""REST API for DuckDB email body storage.

Provides centralized read/write access to DuckDB, solving concurrency issues
since DuckDB doesn't support multiple concurrent connections.
"""

import base64
import json
import logging
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import duckdb
import html2text
import uvicorn
from cortex_utils.metrics import ERRORS, start_metrics_server
from fastapi import FastAPI, HTTPException, Query, Request
from prometheus_client import REGISTRY, Counter, Gauge, Histogram
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

logger = logging.getLogger(__name__)

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/app/data/emails.duckdb")
HOST = os.environ.get("DUCKDB_API_HOST", "0.0.0.0")
PORT = int(os.environ.get("DUCKDB_API_PORT", "8081"))


def _get_or_create_metric(
    metric_class: type,
    name: str,
    description: str,
    labelnames: list[str] | None = None,
    **kwargs: Any,
) -> Any:
    """Get existing metric or create new one (handles uvicorn reload)."""
    if labelnames is None:
        labelnames = []
    try:
        return metric_class(name, description, labelnames, **kwargs)
    except ValueError:
        # Metric already registered - get it from registry
        # Try full name first, then without _total suffix (for Counters)
        # Note: Uses private _names_to_collectors as no public API exists for this
        return REGISTRY._names_to_collectors.get(
            name
        ) or REGISTRY._names_to_collectors.get(name.removesuffix("_total"))


# HTTP metrics (guarded against duplicate registration)
HTTP_REQUESTS = _get_or_create_metric(
    Counter,
    "duckdb_api_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
HTTP_REQUEST_DURATION = _get_or_create_metric(
    Histogram,
    "duckdb_api_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# DuckDB metrics
DUCKDB_QUERIES = _get_or_create_metric(
    Counter,
    "duckdb_api_queries_total",
    "Total DuckDB queries executed",
    ["operation"],
)
DUCKDB_QUERY_DURATION = _get_or_create_metric(
    Histogram,
    "duckdb_api_query_duration_seconds",
    "DuckDB query latency",
    ["operation"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
DUCKDB_ROWS = _get_or_create_metric(
    Gauge,
    "duckdb_api_rows_total",
    "Total rows in email_bodies table",
)
DUCKDB_DATABASE_SIZE = _get_or_create_metric(
    Gauge,
    "duckdb_api_database_size_bytes",
    "Size of DuckDB database file",
)

# Configure html2text for clean markdown output
_html2text = html2text.HTML2Text()
_html2text.ignore_links = False  # Keep link URLs as markdown [text](url)
_html2text.ignore_images = True  # Skip image references
_html2text.ignore_emphasis = False  # Keep **bold** and *italic* markdown
_html2text.body_width = 0  # Don't wrap lines
_html2text.single_line_break = True  # More compact output


def _decode_base64_urlsafe(data: str) -> str:
    """Decode base64 URL-safe encoded data (Gmail API format)."""
    # Gmail uses URL-safe base64 without padding
    # Add padding if needed
    padded = data + "=" * (4 - len(data) % 4) if len(data) % 4 else data
    try:
        return base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
    except (ValueError, UnicodeDecodeError):
        return ""


def _html_to_markdown(html_content: str) -> str:
    """Convert HTML to markdown using html2text."""
    result: str = _html2text.handle(html_content)
    return result.strip()


def _looks_like_html(text: str) -> bool:
    """Check if text appears to contain significant HTML markup.

    Some email senders incorrectly label HTML content as text/plain.
    This detects that case so we can run the content through html2text.

    Uses conservative thresholds to avoid false positives:
    - Minimum 50 chars (very short text can't be meaningful HTML)
    - Check first 8KB (some emails have long tracking URLs before HTML content)
    - Require 3+ HTML tags (avoids false positives like "value < 10" or XML snippets)
    - Require 2+ HTML-specific indicators (distinguishes from generic angle brackets)
    """
    # Minimum length check - very short text can't be meaningful HTML
    if not text or len(text) < 50:
        return False

    # Sample first 8KB - some emails have long tracking URLs before HTML content
    sample = text[:8000]

    # Fast path: if no angle brackets, definitely not HTML
    if "<" not in sample:
        return False

    # Count HTML-like tags (require 3+ to avoid false positives)
    tags = re.findall(r"<[a-zA-Z][^>]*>", sample)
    if len(tags) < 3:
        return False

    # Check for HTML-specific patterns (require 2+ indicators)
    html_indicators = [
        "<!DOCTYPE",
        "<html",
        "<body",
        "<div",
        "<span",
        "<table",
        "<p>",
        "<br",
        'class="',
        'style="',
    ]
    sample_lower = sample.lower()
    indicator_count = sum(1 for ind in html_indicators if ind.lower() in sample_lower)

    # Both conditions must be met to classify as HTML
    return len(tags) >= 3 and indicator_count >= 2


def _extract_body_text(parts: Any) -> str | None:
    """Extract text body from Gmail message parts (markdown format).

    Looks for text/plain first, falls back to text/html (converted to markdown).
    Handles nested multipart messages using iterative DFS.

    Some email senders incorrectly label HTML content as text/plain.
    We detect this and convert it properly using html2text.

    Returns markdown-formatted text (preserving bold/italic/links).
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
            # Use first text/plain part found (don't overwrite with later parts)
            if body_data and not plain_body_data:
                plain_body_data = body_data

        elif mime_type == "text/html" and not html_body_data:
            # Store first HTML part as fallback if no plain text
            body_data = part.get("body", {}).get("data")
            if body_data:
                html_body_data = body_data

    if plain_body_data:
        plain_text = _decode_base64_urlsafe(plain_body_data)
        # Check if "plain" text is actually mislabeled HTML
        if _looks_like_html(plain_text):
            try:
                return _html_to_markdown(plain_text)
            except Exception:
                logger.warning(
                    "html2text failed on mislabeled HTML, returning raw text"
                )
                return plain_text
        return plain_text

    if html_body_data:
        html_content = _decode_base64_urlsafe(html_body_data)
        try:
            return _html_to_markdown(html_content)
        except Exception:
            logger.warning("html2text failed on HTML content, returning None")
            return None

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


def _execute_query(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
    operation: str = "read",
) -> Any:
    """Execute a query with timing metrics."""
    start = time.time()
    if params:
        result = conn.execute(sql, params)
    else:
        result = conn.execute(sql)
    duration = time.time() - start
    DUCKDB_QUERIES.labels(operation=operation).inc()
    DUCKDB_QUERY_DURATION.labels(operation=operation).observe(duration)
    return result


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


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware to record HTTP request metrics."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start_time = time.time()
        response: Response = await call_next(request)
        duration = time.time() - start_time

        # Use route path template to avoid cardinality explosion with path params
        route = request.scope.get("route")
        endpoint = route.path if route else request.url.path

        # Skip metrics endpoint to avoid recursion
        if endpoint != "/metrics":
            HTTP_REQUESTS.labels(
                method=request.method,
                endpoint=endpoint,
                status=response.status_code,
            ).inc()
            HTTP_REQUEST_DURATION.labels(
                method=request.method,
                endpoint=endpoint,
            ).observe(duration)

            # Track errors
            if response.status_code >= 500:
                ERRORS.labels(service="duckdb-api", error_type="http_5xx").inc()

        return response


app.add_middleware(MetricsMiddleware)  # type: ignore[arg-type,call-arg]


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
        row = _execute_query(
            conn, "SELECT COUNT(*) FROM email_bodies", operation="read"
        ).fetchone()
    count = row[0] if row else 0

    # Update gauges (defensive - may be None in edge cases)
    if DUCKDB_ROWS is not None:
        DUCKDB_ROWS.set(count)
    else:
        logger.warning("DUCKDB_ROWS metric not available, skipping update.")
    if DUCKDB_DATABASE_SIZE is not None:
        if os.path.exists(DUCKDB_PATH):
            DUCKDB_DATABASE_SIZE.set(os.path.getsize(DUCKDB_PATH))
    else:
        logger.warning("DUCKDB_DATABASE_SIZE metric not available, skipping update.")

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
    """Get text content of an email (markdown format).

    Returns decoded text body. If the email has only HTML (or text/plain
    that is actually mislabeled HTML), the content is converted to markdown
    preserving bold/italic formatting and links.
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
    start = time.time()
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
    duration = time.time() - start
    DUCKDB_QUERIES.labels(operation="write").inc()
    DUCKDB_QUERY_DURATION.labels(operation="write").observe(duration)

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
    # Start metrics server on separate port
    metrics_port = int(os.environ.get("METRICS_PORT", "8003"))
    start_metrics_server(port=metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")

    uvicorn.run(
        "postmark.services.duckdb_api:app",
        host=HOST,
        port=PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
