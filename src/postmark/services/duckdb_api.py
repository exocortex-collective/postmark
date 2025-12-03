"""Simple REST API for DuckDB email body queries."""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/app/data/emails.duckdb")
HOST = os.environ.get("DUCKDB_API_HOST", "0.0.0.0")
PORT = int(os.environ.get("DUCKDB_API_PORT", "8081"))


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a read-only DuckDB connection."""
    return duckdb.connect(DUCKDB_PATH, read_only=True)


class DuckDBHandler(BaseHTTPRequestHandler):
    """Handle REST requests for DuckDB queries."""

    def _send_json(self, data: dict, status: int = 200) -> None:
        """Send JSON response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def _send_error(self, message: str, status: int = 400) -> None:
        """Send error response."""
        self._send_json({"error": message}, status)

    def do_GET(self) -> None:
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/health":
            self._send_json({"status": "ok"})
            return

        if path == "/body":
            gmail_ids_list = query.get("gmail_id", [])
            gmail_id = gmail_ids_list[0] if gmail_ids_list else None
            if not gmail_id:
                self._send_error("Missing gmail_id parameter")
                return

            try:
                conn = get_connection()
                result = conn.execute(
                    "SELECT parts, body_data FROM email_bodies WHERE gmail_id = ?",
                    [gmail_id],
                ).fetchone()
                conn.close()

                if result:
                    self._send_json(
                        {
                            "gmail_id": gmail_id,
                            "parts": result[0],
                            "body_data": result[1],
                        }
                    )
                else:
                    self._send_error(f"No body found for gmail_id: {gmail_id}", 404)

            except Exception as e:
                self._send_error(str(e), 500)
            return

        if path == "/bodies":
            # Batch fetch - comma-separated gmail_ids
            gmail_ids_param = query.get("gmail_ids", [])
            gmail_ids_str = gmail_ids_param[0] if gmail_ids_param else None
            if not gmail_ids_str:
                self._send_error("Missing gmail_ids parameter")
                return

            gmail_ids = [g.strip() for g in gmail_ids_str.split(",")]
            if len(gmail_ids) > 100:
                self._send_error("Max 100 gmail_ids per request")
                return

            try:
                conn = get_connection()
                placeholders = ",".join(["?" for _ in gmail_ids])
                rows = conn.execute(
                    f"SELECT gmail_id, parts, body_data FROM email_bodies WHERE gmail_id IN ({placeholders})",
                    gmail_ids,
                ).fetchall()
                conn.close()

                bodies = {
                    row[0]: {"parts": row[1], "body_data": row[2]} for row in rows
                }
                self._send_json({"bodies": bodies})

            except Exception as e:
                self._send_error(str(e), 500)
            return

        if path == "/stats":
            try:
                conn = get_connection()
                row = conn.execute("SELECT COUNT(*) FROM email_bodies").fetchone()
                conn.close()
                count = row[0] if row else 0
                self._send_json({"count": count})
            except Exception as e:
                self._send_error(str(e), 500)
            return

        self._send_error("Not found", 404)

    def log_message(self, format: str, *args: object) -> None:
        """Suppress default logging."""
        pass


def main() -> None:
    """Run the DuckDB API server."""
    server = HTTPServer((HOST, PORT), DuckDBHandler)
    print(f"DuckDB API listening on {HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")
        server.shutdown()


if __name__ == "__main__":
    main()
