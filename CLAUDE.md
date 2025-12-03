# Postmark - Gmail Sync & Storage

Gmail integration layer for Cortex. Syncs email via GCP Pub/Sub push notifications.

**Single-user homelab deployment only.** One instance = one Gmail account. See `docs/project-spec.md` for design philosophy.

**Scale assumption:** The target Gmail account has ~70GB of mail (millions of messages). Design for this scale - avoid loading entire datasets into memory, use pagination/streaming where appropriate.

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

## Git Workflow

**All changes must go through PRs - never push directly to main.**

1. Create a feature branch: `git checkout -b feature-name`
2. Make changes and commit
3. Push branch and create PR: `git push -u origin feature-name && gh pr create`
4. Wait for CI to pass
5. Merge via GitHub (squash or merge commit)

## Deployment

Two containers run together:
- `cortex-duckdb-api` - DuckDB API server (holds write lock on DuckDB)
- `cortex-gmail-sync` - Gmail sync service (connects to DuckDB API)

Docker images: `us-central1-docker.pkg.dev/cortex-gmail/cortex/postmark:latest`
Infrastructure: GCP resources managed in `cortex-infra` repo
CI/CD: GitHub Actions with Workload Identity Federation (keyless GCP auth)

### Portainer Stack Management

The cortex-postmark stack is managed via Portainer on the **hades** endpoint (ID 11, 10.5.2.21:9001).

**Important:** Use endpoint ID 11 (hades) for 10.5.2.21, NOT endpoint 3 (ares which is local).

Portainer API: `https://localhost:9443`
API key location: `~/HomeLab/.env` (PORTAINER_ACCESS_TOKEN)

```bash
# List stacks on hades
curl -k -s 'https://localhost:9443/api/stacks' \
  -H "X-API-Key: $PORTAINER_ACCESS_TOKEN" | jq '.[] | select(.EndpointId == 11)'

# Create/update stack (use endpointId=11 for hades)
curl -k -s -X POST 'https://localhost:9443/api/stacks/create/standalone/file?endpointId=11' \
  -H "X-API-Key: $PORTAINER_ACCESS_TOKEN" \
  -F 'Name=cortex-postmark' \
  -F 'file=@docker-compose.yml'

# Redeploy existing stack
curl -k -s -X PUT 'https://localhost:9443/api/stacks/{id}?endpointId=11' \
  -H "X-API-Key: $PORTAINER_ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"stackFileContent": "...", "pullImage": true}'

# Stop/start stack
curl -k -s -X POST 'https://localhost:9443/api/stacks/{id}/stop?endpointId=11' \
  -H "X-API-Key: $PORTAINER_ACCESS_TOKEN"
curl -k -s -X POST 'https://localhost:9443/api/stacks/{id}/start?endpointId=11' \
  -H "X-API-Key: $PORTAINER_ACCESS_TOKEN"
```

**Registry Authentication:** GCP Artifact Registry is configured in Portainer (registry ID 1). Ensure docker is authenticated on the target host before Portainer can pull:
```bash
ssh devon@10.5.2.21 "cat /home/devon/cortex-secrets/gcp-service-account.json | docker login -u _json_key --password-stdin https://us-central1-docker.pkg.dev"
```

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

## Code Review Workflow

When checking PR reviews, only fetch **unresolved** comments - resolved conversations indicate completed items:

```bash
# Get unresolved review threads
gh api graphql -f query='
query($owner: String!, $repo: String!, $pr: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100) {
        nodes {
          isResolved
          comments(first: 10) {
            nodes { body path line }
          }
        }
      }
    }
  }
}' -f owner=devonjones -f repo=postmark -F pr=24 --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)'
```
