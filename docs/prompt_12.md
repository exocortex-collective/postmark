**Context**
- API service runs.
- Add metrics and readiness.

**Task**
1. Integrate `prometheus-client`:
   - `counter_requests_total`, `histogram_latency_seconds`.
2. Mount `/metrics` endpoint.
3. Add `/ready` that checks SQLite connection & S3 HEAD bucket.
4. Write `docs/quickstart.md` describing Docker dev workflow.

**Tests**
- Hitting `/metrics` shows numeric lines.
- `/ready` returns 500 if DB is closed.

Write the code and the tests.
