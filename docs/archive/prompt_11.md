**Context**
- Store is populated & kept fresh.
- Provide query interface for AI agents.

**Task**
1. Add `postmark/mcp/api.py` with FastAPI app.
2. Endpoints:
   - `GET /health` returns 200.
   - `POST /search` body `{"query": "...", "page_size": 20}`.
   - `POST /get_email` body `{"uuid": "...", "include_body": true}`.
3. Use SQLite `LIKE` queries for PoC; paginate with limit/offset.

**Tests**
- Launch test client; `/health` returns OK.
- Index sample mails then `/search` returns expected UUIDs.

Write the code and the tests.
