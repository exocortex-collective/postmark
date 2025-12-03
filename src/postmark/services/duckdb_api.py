"""REST API for DuckDB email body storage.

Provides centralized read/write access to DuckDB, solving concurrency issues
since DuckDB doesn't support multiple concurrent connections.
"""

import json
import os
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


class StatusResponse(BaseModel):
    status: str


class StatsResponse(BaseModel):
    count: int


class BodiesResponse(BaseModel):
    bodies: dict[str, dict[str, Any]]


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
    return BodyResponse(
        gmail_id=gmail_id,
        parts=parts_data,
        body_data=result[1],
    )


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

    # Deserialize JSON parts for cleaner API response
    bodies = {
        row[0]: {
            "parts": json.loads(row[1]) if row[1] else None,
            "body_data": row[2],
        }
        for row in rows
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
