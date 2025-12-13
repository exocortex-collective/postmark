# Postmark - Gmail Sync & Storage

**Project**: cortex-postmark
**Purpose**: Gmail integration layer - real-time sync, mail storage, and future backfill capabilities

## Overview

Postmark is the email ingestion service for Cortex. It receives push notifications from Gmail via GCP Pub/Sub, fetches new messages using the Gmail History API, and stores them for downstream processing by other Cortex services (triage, hoard, etc.).

## Design Philosophy

**This is explicitly designed for single-user homelab deployments.** Postmark is intended for individuals who want to run their own email triage system with full control over their infrastructure and data.

Key assumptions:
- **Single Gmail account**: One deployment handles one Gmail account
- **Self-hosted**: Runs on your own hardware (homelab, VPS, etc.)
- **Full asset control**: You manage your own GCP project, Postgres, secrets

If you want to monitor multiple Gmail accounts, each account requires its own:
- OAuth credentials and token
- GCP Pub/Sub topic/subscription
- Deployment instance

This is intentional - it keeps the system simple and avoids the complexity of multi-tenant authentication, while giving you complete ownership of your email data.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Gmail API                             │
│        (watch + history.list + messages.get + labels)        │
└────────────────┬────────────────────────────────────────────┘
                 │ Push notification (on mailbox change)
                 ▼
         ┌───────────────┐
         │  GCP Pub/Sub  │  (Google Cloud)
         │  Topic:       │
         │  gmail-events │
         └───────┬───────┘
                 │
                 ▼
         ┌───────────────┐
         │  Pub/Sub      │  (pull subscription)
         │  Subscription │
         │  gmail-sync   │
         └───────┬───────┘
                 │ Long-poll (sub-second latency)
                 ▼
         ┌───────────────────────────────────────────────────┐
         │             gmail-sync (Python daemon)            │
         │  - Pulls Pub/Sub subscription                     │
         │  - Processes history: messageAdded, labelAdded,   │
         │    labelRemoved, messageDeleted                   │
         │  - Stores raw API response (no parsing)           │
         │  - Syncs Gmail labels to gmail_labels table       │
         │  - Renews watch daily, hourly fallback poll       │
         └───────┬──────────────────────────────────────────┘
                 │
        ┌────────┼────────┬──────────────┐
        ▼        ▼        ▼
┌─────────────┐ ┌─────────────┐ ┌───────────────┐
│  Postgres   │ │  DuckDB API │ │ Queue: parse  │
│ emails_raw  │ │ (REST API)  │ │ Queue: attach │
│ gmail_labels│ └──────┬──────┘ └───────┬───────┘
│ attachments │        ▼                │
│ sync_state  │ ┌─────────────┐         │
└─────────────┘ │   DuckDB    │         │
                │ email_bodies│         │
                └─────────────┘         │
                                        ▼
         ┌──────────────────────────────────────────────────┐
         │                Workers (parallel)                │
         │  ┌──────────────┐  ┌───────────────────────────┐ │
         │  │ parse-worker │  │ attachment-worker         │ │
         │  │ Headers→     │  │ Gmail API→S3/Minio        │ │
         │  │ emails_parsed│  │ Updates attachments table │ │
         │  └──────┬───────┘  └───────────────────────────┘ │
         └─────────┼──────────────────────────────────────────┘
                   │
                   │ Queue: 'triage'
                   ▼
         ┌───────────────┐
         │    triage     │  (separate service)
         │ - Rules engine│
         │ - LLM fallback│
         │ - Gmail labels│
         └───────────────┘
```

## Services

postmark runs as 4 Docker containers:

| Service | Container | Purpose |
|---------|-----------|---------|
| gmail-sync | cortex-postmark-gmail-sync | Pub/Sub listener, Gmail API, stores raw emails |
| duckdb-api | cortex-postmark-duckdb-api | REST API for DuckDB body queries |
| parse-worker | cortex-postmark-parse-worker | Header parsing, emails_parsed |
| attachment-worker | cortex-postmark-attachment-worker | Fetches attachments, stores in S3 |

The **duckdb-api** holds the write lock on DuckDB and exposes endpoints:
- `GET /health` - Health check
- `GET /stats` - Email count
- `GET /body?gmail_id=X` - Single body
- `POST /body` - Store an email body
- `GET /bodies?gmail_ids=X,Y,Z` - Batch body fetch
- `GET /gmail_ids` - List all gmail IDs in DuckDB
- `GET /mail_text?gmail_id=X` - Extract plain text from email
- `POST /checkpoint` - Force a WAL checkpoint

## Gmail Push Notification Flow

1. **Initial setup**: Call `users.watch()` to register for push notifications
   - Returns `historyId` as baseline for tracking changes
   - Must renew every 7 days (service renews daily)

2. **On mailbox change**: Gmail publishes to Pub/Sub topic
   - Payload: `{"emailAddress": "user@example.com", "historyId": "12345"}`
   - Rate limited to 1 notification/second (aggregated if faster)

3. **gmail-sync receives notification**: Long-polls Pub/Sub subscription
   - Sub-second latency when messages waiting
   - 30-second timeout when idle, then re-poll

4. **Fetch changes**: Call `history.list(startHistoryId=X)`
   - Returns `messageAdded`, `messageDeleted`, `labelAdded`, `labelRemoved` events
   - Process only `messageAdded` for new email ingestion

5. **Fallback**: Hourly full poll catches any missed notifications
   - Pub/Sub is reliable but not guaranteed
   - History API handles gaps gracefully

## Data Model

The data model separates raw API storage from parsed data:

1. **gmail-sync** stores raw API response (fast, never fails)
2. **parse-worker** extracts structured fields (can fail, retry, iterate)

This allows reprocessing without refetching from Gmail API.

### Postgres Tables

```sql
-- Raw email storage (written by gmail-sync)
CREATE TABLE emails_raw (
    id BIGSERIAL PRIMARY KEY,
    gmail_id TEXT UNIQUE NOT NULL,
    thread_id TEXT NOT NULL,
    history_id TEXT NOT NULL,
    label_ids TEXT[],               -- Gmail label IDs (updated on label events)
    internal_date BIGINT NOT NULL,  -- ms since epoch from Gmail
    size_estimate INT,
    snippet TEXT,
    headers JSONB NOT NULL,         -- Raw headers array from API
    mime_type TEXT,
    attachment_ids TEXT[],          -- For separate attachment fetch queue
    raw_response JSONB,             -- Full API response minus body
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ          -- Soft delete (set when Gmail message deleted)
);

CREATE INDEX idx_emails_raw_gmail_id ON emails_raw(gmail_id);
CREATE INDEX idx_emails_raw_thread ON emails_raw(thread_id);
CREATE INDEX idx_emails_raw_date ON emails_raw(internal_date DESC);

-- Gmail label mapping (synced from Gmail API)
CREATE TABLE gmail_labels (
    id TEXT PRIMARY KEY,            -- Gmail label ID (e.g., "Label_123")
    name TEXT NOT NULL,             -- Full label name (e.g., "Cortex/Uncategorized")
    name_parts TEXT[] NOT NULL,     -- Path parts for hierarchy queries
    type TEXT CHECK (type IN ('system', 'user')),  -- 'system' or 'user'
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_gmail_labels_name ON gmail_labels(name);
CREATE INDEX idx_gmail_labels_parts ON gmail_labels USING GIN(name_parts);

-- Attachment metadata (written by attachment-worker)
CREATE TABLE attachments (
    id BIGSERIAL PRIMARY KEY,
    gmail_id TEXT NOT NULL REFERENCES emails_raw(gmail_id) ON DELETE CASCADE,
    attachment_id TEXT NOT NULL,
    filename TEXT,
    mime_type TEXT,
    size_bytes INT,
    s3_bucket TEXT NOT NULL,        -- S3/Minio bucket
    s3_key TEXT NOT NULL,           -- S3/Minio object key
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(gmail_id, attachment_id)
);

CREATE INDEX idx_attachments_gmail_id ON attachments(gmail_id);

-- Parsed email metadata (written by parse-worker)
CREATE TABLE emails_parsed (
    id BIGSERIAL PRIMARY KEY,
    gmail_id TEXT UNIQUE NOT NULL REFERENCES emails_raw(gmail_id),
    from_addr TEXT,
    from_name TEXT,
    to_addrs TEXT[],
    cc_addrs TEXT[],
    bcc_addrs TEXT[],
    subject TEXT,
    date_header TIMESTAMPTZ,        -- Parsed from Date header
    message_id TEXT,                -- Message-ID header
    in_reply_to TEXT,               -- In-Reply-To header
    references TEXT[],              -- References header (for threading)
    parsed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_emails_parsed_gmail_id ON emails_parsed(gmail_id);
CREATE INDEX idx_emails_parsed_from ON emails_parsed(from_addr);
CREATE INDEX idx_emails_parsed_date ON emails_parsed(date_header DESC);

-- Sync state tracking
CREATE TABLE sync_state (
    id BIGSERIAL PRIMARY KEY,
    email_address TEXT UNIQUE NOT NULL,
    last_history_id TEXT NOT NULL,
    watch_expiration TIMESTAMPTZ,
    last_sync_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Generic queue for downstream processing
CREATE TABLE queue (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,       -- 'parse', 'triage', 'attachment'
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

CREATE INDEX idx_queue_pending ON queue(queue_name, status, created_at)
    WHERE status = 'pending';
CREATE INDEX idx_queue_pending_gmail_id ON queue((payload->>'gmail_id'))
    WHERE status = 'pending';
```

### DuckDB (Email Bodies)

Body content stored separately - only accessed when reading messages or reprocessing:

```sql
-- DuckDB schema (separate from Postgres)
CREATE TABLE email_bodies (
    gmail_id TEXT PRIMARY KEY,
    parts JSON,  -- Full MIME parts structure from API
    body_data TEXT,  -- Base64 encoded body from API
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Service: gmail-sync

### Purpose

Daemon that syncs Gmail to local storage via Pub/Sub push notifications.

### Key Features

- Long-polls GCP Pub/Sub subscription for near real-time updates
- Calls Gmail history.list API on notification (efficient, only fetch changes)
- Renews Gmail watch registration daily
- Hourly fallback poll catches any missed notifications
- Stores metadata in Postgres, bodies in DuckDB
- Enqueues new emails for downstream processing (triage)
- Idempotent (safe to re-run, no duplicates)

### Configuration

Environment variables (passed to Docker container):

```bash
# Gmail API (OAuth)
GMAIL_TOKEN_PATH=/app/secrets/gmail-token.json

# GCP (Pub/Sub)
GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-service-account.json
GCP_PROJECT_ID=cortex-gmail
PUBSUB_SUBSCRIPTION=projects/cortex-gmail/subscriptions/gmail-sync
PUBSUB_TOPIC=projects/cortex-gmail/topics/gmail-events

# Postgres
POSTGRES_HOST=${POSTGRES_HOST}
POSTGRES_PORT=5432
POSTGRES_DB=cortex
POSTGRES_USER=cortex
POSTGRES_PASSWORD=<from-secret>

# DuckDB
DUCKDB_PATH=/app/data/emails.duckdb

# Sync settings
FALLBACK_POLL_INTERVAL=3600  # seconds
WATCH_RENEWAL_INTERVAL=86400  # daily
```

### Pseudocode

```python
from google.cloud import pubsub_v1
import threading

subscriber = pubsub_v1.SubscriberClient()
subscription_path = os.environ["PUBSUB_SUBSCRIPTION"]

def process_notification(message):
    """Handle a single Pub/Sub notification."""
    data = json.loads(base64.urlsafe_b64decode(message.data))
    new_history_id = data['historyId']
    email_address = data['emailAddress']

    last_history_id = db.get_last_history_id(email_address)

    try:
        history = gmail.users().history().list(
            userId='me',
            startHistoryId=last_history_id,
            historyTypes=['messageAdded']
        ).execute()

        for record in history.get('history', []):
            for msg_added in record.get('messagesAdded', []):
                msg_id = msg_added['message']['id']
                store_raw_message(msg_id)

        db.set_last_history_id(email_address, history['historyId'])
        message.ack()

    except HttpError as e:
        if e.resp.status == 404:
            # History too old, need full sync
            trigger_full_sync(email_address)
            message.ack()
        else:
            message.nack()  # Retry

def store_raw_message(gmail_id):
    """Fetch and store raw message - no parsing, just slam it in."""
    if db.email_exists(gmail_id):
        return  # Idempotent

    msg = gmail.users().messages().get(
        userId='me', id=gmail_id, format='full'
    ).execute()

    # Extract attachment IDs from parts (if any)
    attachment_ids = extract_attachment_ids(msg.get('payload', {}))

    # Store structured fields in Postgres (no parsing!)
    db.insert_email_raw({
        'gmail_id': gmail_id,
        'thread_id': msg['threadId'],
        'history_id': msg['historyId'],
        'label_ids': msg.get('labelIds', []),
        'internal_date': msg['internalDate'],
        'size_estimate': msg.get('sizeEstimate'),
        'snippet': msg.get('snippet', ''),
        'headers': msg['payload']['headers'],  # Raw JSONB
        'mime_type': msg['payload'].get('mimeType'),
        'attachment_ids': attachment_ids,
        'raw_response': strip_body(msg),  # Full response minus body for debugging
    })

    # Store body in DuckDB
    duckdb.insert_body({
        'gmail_id': gmail_id,
        'parts': msg['payload'].get('parts'),
        'body_data': msg['payload'].get('body', {}).get('data'),
    })

    # Enqueue for parsing (not classification - that comes after parsing)
    db.enqueue('parse', {'gmail_id': gmail_id})

    # Enqueue attachments for separate fetch
    for att_id in attachment_ids:
        db.enqueue('attachment', {'gmail_id': gmail_id, 'attachment_id': att_id})

def renew_watch():
    """Renew Gmail push notification registration."""
    response = gmail.users().watch(
        userId='me',
        body={
            'topicName': os.environ['PUBSUB_TOPIC'],
            'labelIds': ['INBOX'],
            'labelFilterBehavior': 'INCLUDE'
        }
    ).execute()
    db.update_watch_expiration(response['expiration'])

def main():
    renew_watch()

    schedule.every().day.at("00:00").do(renew_watch)
    schedule.every().hour.do(fallback_poll)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=process_notification
    )

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    streaming_pull_future.result()  # Block forever
```

## Service: parse-worker

### Purpose

Worker that reads from the `parse` queue, extracts structured fields from raw headers, and enqueues for classification. Designed to fail fast - if parsing fails, log the error and move on.

### Key Features

- Reads raw headers JSONB from `emails_raw`
- Parses RFC2047 encoded headers (From, To, Subject, etc.)
- Extracts email addresses and display names
- Writes structured data to `emails_parsed`
- Enqueues for classification (triage service)
- Fail-fast: logs errors, marks job failed, continues processing

### Pseudocode

```python
def process_parse_job(job):
    """Parse a single email's headers."""
    gmail_id = job['payload']['gmail_id']

    try:
        raw = db.get_email_raw(gmail_id)
        headers = {h['name'].lower(): h['value'] for h in raw['headers']}

        # Parse with fail-fast - any exception marks job failed
        from_addr, from_name = parse_address(headers.get('from', ''))
        to_addrs = parse_address_list(headers.get('to', ''))
        cc_addrs = parse_address_list(headers.get('cc', ''))
        bcc_addrs = parse_address_list(headers.get('bcc', ''))
        subject = decode_header(headers.get('subject', ''))
        date_header = parse_date(headers.get('date', ''))

        db.insert_email_parsed({
            'gmail_id': gmail_id,
            'from_addr': from_addr,
            'from_name': from_name,
            'to_addrs': to_addrs,
            'cc_addrs': cc_addrs,
            'bcc_addrs': bcc_addrs,
            'subject': subject,
            'date_header': date_header,
            'message_id': headers.get('message-id'),
            'in_reply_to': headers.get('in-reply-to'),
            'references': parse_references(headers.get('references', '')),
        })

        # Now enqueue for classification
        db.enqueue('classify', {'gmail_id': gmail_id})
        db.complete_job(job['id'])

    except Exception as e:
        # Fail fast - log and move on
        db.fail_job(job['id'], str(e))
        logger.error(f"Failed to parse {gmail_id}: {e}")
```

## Docker Deployment

### Dockerfile

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy source code
COPY src/ ./src/

# Create data directory for DuckDB
RUN mkdir -p /app/data

# Secrets will be mounted at runtime
VOLUME /app/secrets
VOLUME /app/data

# Run the gmail-sync service
CMD ["uv", "run", "python", "-m", "postmark.services.gmail_sync"]
```

### Portainer Stack

Deployed as `cortex-postmark` stack on Hades via Portainer:

```yaml
version: '3.8'
services:
  gmail-sync:
    image: us-central1-docker.pkg.dev/cortex-gmail/cortex/postmark:latest
    container_name: cortex-postmark-gmail-sync
    restart: unless-stopped
    environment:
      - GMAIL_TOKEN_PATH=/app/secrets/gmail-token.json
      - GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-service-account.json
      - GCP_PROJECT_ID=cortex-gmail
      - PUBSUB_SUBSCRIPTION=projects/cortex-gmail/subscriptions/gmail-sync
      - PUBSUB_TOPIC=projects/cortex-gmail/topics/gmail-events
      - POSTGRES_HOST=${POSTGRES_HOST}
      - POSTGRES_PORT=5432
      - POSTGRES_DB=cortex
      - POSTGRES_USER=cortex
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - DUCKDB_PATH=/app/data/emails.duckdb
    volumes:
      - /home/devon/cortex-secrets/gmail-token.json:/app/secrets/gmail-token.json:ro
      - /home/devon/cortex-secrets/gcp-service-account.json:/app/secrets/gcp-service-account.json:ro
      - postmark-data:/app/data
    networks:
      - cortex

volumes:
  postmark-data:

networks:
  cortex:
    external: true
```

## GCP Infrastructure

Managed via Terraform in `cortex-infra` repo.

### Resources

- **Project**: `cortex-gmail`
- **Pub/Sub Topic**: `projects/cortex-gmail/topics/gmail-events`
- **Pub/Sub Subscription**: `projects/cortex-gmail/subscriptions/gmail-sync` (pull)
- **Service Account**: `gmail-sync@cortex-gmail.iam.gserviceaccount.com`
  - Role: `roles/pubsub.subscriber` on subscription

### Gmail API Permissions

- Gmail watch() publishes to Pub/Sub via `gmail-api-push@system.gserviceaccount.com`
- This service account needs `roles/pubsub.publisher` on the topic

## Future Capabilities

### Backfill from Takeout

Archived code in `src/postmark/archive/` supports batch ingestion from Gmail takeout exports (mbox.gz files). This can be revived for:
- Initial historical import
- Disaster recovery
- Migrating additional accounts

### Multi-Account Support

Schema supports multiple email accounts via `sync_state.email_address`. Future work:
- Add work email (Valid Eval)
- Separate watch registrations per account
- Account-specific configuration

### Full-Text Search

DuckDB bodies enable efficient full-text search. Future integration with:
- Obsidian search
- MCP server for Claude queries

## Testing

```bash
# Run tests
uv run pytest

# Type checking
uv run mypy src/postmark

# Lint
uv run black --check .
uv run isort --check-only .
```

## Related Documentation

- [Cortex Overview](../../docs/cortex-overview.md) - Project context
- [MVP Specification](../../docs/mvp-spec-phase1.md) - Full system spec
- [GCP Setup](../../docs/mvp-spec-phase1.md#gcp-setup-pubsub-for-gmail-push) - Detailed GCP setup steps
