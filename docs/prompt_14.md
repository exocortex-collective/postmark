**Context**
- All Python components exist with tests.
- Need deployable container and CI.

**Task**
1. Write `Dockerfile`:
   - `FROM python:3.10-slim`
   - Install system deps (`libpq-dev`, `build-essential`), copy code, run `uv pip install`.
   - `CMD ["postmark-mcp"]`
2. Add `docker-compose.yml` with services: `mcp`, `minio`, `prometheus`.
3. GitHub Actions workflow:
   - Steps: checkout, set up Python, run `uv pip install -e ".[dev]"`, `pytest`, build/push Docker image.
4. Update `README.md` with badge and `run.sh` example.

**Tests**
- CI job passes.
- Local `docker-compose up mcp` exposes `/health`.

Write the code and the tests.
