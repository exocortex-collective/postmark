# Postmark - Gmail Sync & Storage

Gmail integration layer for Cortex. Syncs email via GCP Pub/Sub push notifications.

**Single-user homelab deployment only.** One instance = one Gmail account. See `docs/project-spec.md` for design philosophy.

## Quick Reference

- **Spec**: `docs/project-spec.md`
- **Main service**: `src/postmark/services/gmail_sync.py`
- **DuckDB API**: `src/postmark/services/duckdb_api.py`
- **Archived code**: `src/postmark/archive/` (Gmail takeout batch processing)

## Project Structure

```
postmark/
├── src/postmark/
│   ├── services/           # Main services (gmail_sync, duckdb_api)
│   ├── storage/            # Postgres, S3, DuckDB clients
│   ├── email/              # Parser, dedup, threading (reusable)
│   └── archive/            # Archived takeout code
├── scripts/                # Dev scripts (backfill.py, parse_test.py)
├── tests/
├── docs/
│   └── project-spec.md     # Full specification
├── pyproject.toml
├── Dockerfile              # gmail-sync service
└── Dockerfile.duckdb-api   # DuckDB API service
```

## Key Dependencies

- `google-cloud-pubsub` - Pub/Sub client
- `google-api-python-client` - Gmail API
- `google-auth-oauthlib` - OAuth flow
- `psycopg2` or `asyncpg` - Postgres
- `duckdb` - Body storage

## Environment Variables

```bash
# Gmail OAuth token (JSON file)
GMAIL_TOKEN_PATH=/app/secrets/gmail-token.json

# GCP service account for Pub/Sub
GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-service-account.json
GCP_PROJECT_ID=cortex-gmail
PUBSUB_SUBSCRIPTION=projects/cortex-gmail/subscriptions/gmail-sync
PUBSUB_TOPIC=projects/cortex-gmail/topics/gmail-events

# Postgres
POSTGRES_HOST=<your-postgres-host>
POSTGRES_PORT=5432
POSTGRES_DB=cortex
POSTGRES_USER=cortex
POSTGRES_PASSWORD=<secret>

# DuckDB API (gmail-sync uses this URL)
DUCKDB_API_URL=http://cortex-duckdb-api:8081

# DuckDB (for duckdb-api service)
DUCKDB_PATH=/app/data/emails.duckdb
```

## Development

```bash
# Setup
uv sync

# Run tests
uv run pytest

# Type check
uv run mypy src/postmark

# Format
uv run black .
uv run isort .
```

## Deployment

Two containers run together:
- `cortex-duckdb-api` - DuckDB API server (holds write lock on DuckDB)
- `cortex-gmail-sync` - Gmail sync service (connects to DuckDB API)

Docker images: `us-central1-docker.pkg.dev/cortex-gmail/cortex/postmark:latest`
Infrastructure: GCP resources managed in `cortex-infra` repo
CI/CD: GitHub Actions with Workload Identity Federation (keyless GCP auth)

## Queue Interface

Postmark enqueues new emails for downstream processing:

```sql
-- Enqueue pattern (consumed by triage)
INSERT INTO queue (queue_name, payload)
VALUES ('classify', '{"email_id": 123, "gmail_id": "abc123"}');
```

## Reusable Components

From archived code, still in use:
- `email/parser.py` - RFC822 parsing
- `email/dedup.py` - Bloom filter deduplication
- `email/threading.py` - Thread detection
- `storage/sqlite.py` - May be adapted for Postgres

## GitHub Integrations

- **Gemini Code Assist**: Automatic code review on PRs
- **Renovate**: Automated dependency updates
