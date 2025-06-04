# Postmark Email DataStore Specification

## Project Overview

Postmark is a high-performance email datastore designed to ingest, store, and expose email data for automation tasks and AI agents. The system processes Gmail exports and maintains ongoing synchronization while providing efficient access through an MCP (Model Context Protocol) interface.

### Goals
- Process and store ~70GB (11.8M emails) from Gmail exports
- Maintain real-time synchronization with Gmail
- Provide fast, flexible access to email data for AI agents
- Support future enrichment workflows (relationship graphs, financial tracking)

### Non-Goals
- This project does not implement enrichment workflows
- Not designed as a general-purpose email client
- No email sending capabilities

## Architecture

### System Components

```
postmark/
├── src/postmark/
│   ├── storage/
│   │   ├── sqlite.py      # Schema versioning, metadata operations
│   │   ├── columnar.py    # Parquet reading/writing
│   │   └── s3.py          # Attachment storage
│   ├── email/
│   │   ├── parser.py      # RFC822 parsing, normalization
│   │   ├── dedup.py       # Fuzzy matching, deduplication logic
│   │   └── threading.py   # Thread detection
│   ├── sync/
│   │   ├── gmail.py       # Gmail API client
│   │   └── imap.py        # IMAP fallback
│   └── __main__.py        # Entry point routing
```

### Core Programs

1. **postmark-backfill**: Processes Gmail export .tgz files
2. **postmark-sync**: Ongoing synchronization service
3. **postmark-mcp**: MCP service for AI agent access
4. **postmark-reindex**: Rebuilds indices with new algorithms
5. **postmark-add-test**: Adds anonymized test cases

### Design Principles

- Functional programming style preferred over OOP
- Functions should be small and single-purpose
- Immutable inputs, return modified outputs
- Process isolation for bulk operations
- Docker-based deployment
- Kubernetes-compatible design

## Data Storage Design

### Storage Layers

1. **SQLite**: Email metadata and indices
2. **Parquet**: Email bodies and headers
3. **S3**: Attachments

### SQLite Schema

```sql
-- Main email table
CREATE TABLE emails (
    id INTEGER PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    message_id TEXT,
    date INTEGER NOT NULL,  -- Unix timestamp
    subject TEXT,
    from_addr TEXT,
    schema_version INTEGER NOT NULL,
    parquet_file TEXT NOT NULL,
    parquet_row INTEGER NOT NULL,
    dedup_method TEXT,  -- 'message_id', 'fuzzy_match', etc.
    thread_id TEXT,
    thread_method TEXT  -- 'headers', 'subject', 'time_window'
);

-- Attachments table
CREATE TABLE attachments (
    id INTEGER PRIMARY KEY,
    email_id INTEGER NOT NULL,
    mime_path TEXT NOT NULL,  -- e.g., "1.2.3"
    filename TEXT,
    size INTEGER NOT NULL,
    content_type TEXT,
    s3_key TEXT NOT NULL,
    content_hash TEXT NOT NULL,  -- SHA256
    upload_timestamp INTEGER NOT NULL,
    s3_version_id TEXT,
    FOREIGN KEY (email_id) REFERENCES emails(id)
);

-- Thread mapping table
CREATE TABLE thread_mapping (
    email_id INTEGER NOT NULL,
    thread_id TEXT NOT NULL,
    confidence REAL,
    method TEXT NOT NULL,
    PRIMARY KEY (email_id, thread_id),
    FOREIGN KEY (email_id) REFERENCES emails(id)
);

-- Sync state table
CREATE TABLE sync_state (
    id INTEGER PRIMARY KEY,
    service TEXT NOT NULL,  -- 'gmail_api', 'imap'
    last_sync_timestamp INTEGER,
    sync_token TEXT,  -- Gmail history ID or IMAP UID
    state TEXT  -- JSON blob for service-specific state
);

-- Dead letter queue
CREATE TABLE dead_letter_queue (
    id INTEGER PRIMARY KEY,
    source_file TEXT NOT NULL,
    file_offset INTEGER NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT,
    traceback TEXT,
    created_at INTEGER NOT NULL
);
```

### Parquet Structure

- **Format**: Apache Parquet with ZSTD compression
- **Partitioning**: By year-month (e.g., `2024-01/`)
- **Row Group Size**: 10,000-50,000 emails per group
- **Target File Size**: ~100-500MB per file

#### Column Schema

```
- uuid: STRING (NOT NULL)
- message_id: STRING
- date: TIMESTAMP
- from: STRING
- to: LIST<STRING>
- cc: LIST<STRING>
- bcc: LIST<STRING>
- subject: STRING
- body_text: STRING
- body_html: STRING
- headers: MAP<STRING, STRING>  -- Additional headers
- raw_size: INT64
- normalized_hash: STRING  -- For deduplication
```

### S3 Attachment Storage

- **Structure**: `{email_uuid}/{mime_path}/{filename}`
- **Example**: `abc-123/1.2/report.pdf`
- **Storage Class**: Standard (migrate to Intelligent-Tiering later)

## Processing Logic

### Email Deduplication

1. **Primary Check**: Bloom filter on Message-ID (0.1% false positive rate)
2. **Secondary Validation**: Fuzzy matching on normalized body
   - Default threshold: 95% similarity
   - Configurable via environment variable

#### Normalization Steps (all toggleable):
1. Strip HTML tags, convert to text
2. Normalize whitespace and line endings
3. Remove quoted reply chains
4. Handle character encoding differences
5. Calculate Levenshtein distance

### Thread Detection

1. **Primary Method**: Use In-Reply-To and References headers
2. **Fallback Methods**:
   - Subject line matching (strip Re:, Fwd:)
   - Time window correlation (messages within 7 days)
3. **Storage**: Track method used for each thread association

### Batch Processing

- **Bulk Loader**: 1000 emails per transaction
- **Parquet Writing**: Batch size of 10,000-50,000 emails
- **Progress Reporting**: Every 1000 emails processed
- **Checkpoint**: Every 10,000 emails with file offset

## Configuration

### Environment Variables (.env)

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
S3_BUCKET=postmark-attachments

# Gmail API
GMAIL_CLIENT_ID=xxx
GMAIL_CLIENT_SECRET=xxx

# Database Paths
SQLITE_PATH=~/.postmark/emails.db
PARQUET_PATH=~/.postmark/parquet/
```

### Tool-Specific YAML Configs

#### ~/.postmark/backfill.yaml
```yaml
processing:
  batch_size: 1000
  parallel_files: 3
  memory_limit_gb: 30

deduplication:
  enabled: true
  threshold: 0.95
  bloom_filter_size: 100000000
  
normalization:
  strip_html: true
  normalize_whitespace: true
  remove_quotes: true
  handle_encodings: true

checkpointing:
  interval: 10000
  manifest_path: ~/.postmark/manifests/
```

#### ~/.postmark/sync.yaml
```yaml
gmail:
  poll_interval: 30
  use_api: true
  fallback_to_imap: true

imap:
  host: imap.gmail.com
  port: 993
  use_ssl: true

error_handling:
  max_retries: 3
  retry_delay: 60
  overlap_window: 3600  # 1 hour
```

#### ~/.postmark/mcp.yaml
```yaml
server:
  port: 8080
  max_connections: 20
  
query_limits:
  default_page_size: 20
  max_page_size: 100
  body_truncate_length: 5000
  
cache:
  enabled: true
  ttl: 300
```

## Error Handling

### Bulk Loader

1. **Failed Emails**: 
   - Write to dead letter queue with .tgz offset
   - Continue processing (unless --fail-fast)
   - Generate error report: `failed_emails_{timestamp}.jsonl`

2. **Recovery Mode**:
   ```bash
   postmark-backfill --resume-from-offset 12345 --process-length 1000
   ```

3. **Manifest Tracking**:
   ```json
   {
     "file": "gmail-export-1.tgz",
     "checksum": "sha256:...",
     "status": "partial",
     "last_offset": 12345678,
     "emails_processed": 45000,
     "errors": 12,
     "timestamp": "2025-01-15T10:30:00Z"
   }
   ```

### Sync Service

1. **Failure Recovery**:
   - Store last successful sync position
   - Overlap window (re-fetch last N messages)
   - Exponential backoff on API errors

2. **Health Checks**:
   - `/health`: Service running
   - `/ready`: Connected to Gmail and databases
   - `/metrics`: Prometheus metrics

## Monitoring

### Prometheus Metrics

```
# Bulk Loader
postmark_emails_processed_total
postmark_emails_deduplicated_total
postmark_processing_errors_total
postmark_processing_duration_seconds
postmark_attachment_bytes_total

# Sync Service
postmark_sync_lag_seconds
postmark_sync_errors_total
postmark_emails_synced_total
postmark_gmail_api_requests_total
postmark_gmail_api_errors_total

# MCP Service
postmark_mcp_requests_total
postmark_mcp_request_duration_seconds
postmark_mcp_active_connections
```

### Logging

- **Format**: Structured JSON
- **Levels**: DEBUG (per email), INFO (batches), ERROR
- **Correlation IDs**: Track email through pipeline
- **Output**: stderr for CLI tools, stdout for services

## MCP Service API

### Endpoints

1. **Search Emails**
   ```json
   {
     "method": "search",
     "params": {
       "query": "from:example@gmail.com",
       "start_date": "2024-01-01",
       "end_date": "2024-12-31",
       "page_size": 20,
       "cursor": "..."
     }
   }
   ```

2. **Retrieve Email**
   ```json
   {
     "method": "get_email",
     "params": {
       "uuid": "abc-123-def",
       "include_body": true,
       "include_attachments": false
     }
   }
   ```

3. **Get Thread**
   ```json
   {
     "method": "get_thread",
     "params": {
       "thread_id": "thread-123",
       "max_messages": 50
     }
   }
   ```

### Response Format

```json
{
  "emails": [
    {
      "uuid": "abc-123",
      "date": "2024-01-15T10:30:00Z",
      "from": "sender@example.com",
      "to": ["recipient@example.com"],
      "subject": "Email subject",
      "body_text": "Truncated to 5KB...",
      "has_attachments": true,
      "thread_id": "thread-123"
    }
  ],
  "metadata": {
    "total_results": 1000,
    "page_size": 20,
    "next_cursor": "..."
  }
}
```

## Testing Strategy

### Test Data Management

1. **Anonymized Real Emails**: Core test dataset
2. **Progressive Anonymization Tool**:
   ```bash
   postmark-add-test problem-email.eml --preserve-error charset-issue
   ```
   - Tries anonymization levels until error reproduces
   - Saves minimal test case
   - Tracks exception signatures

### Test Coverage

- **Unit Tests**: All parsing, normalization, dedup functions
- **Integration Tests**: Full pipeline with MinIO for S3
- **Property-Based Tests**: Fuzzy matching algorithms
- **Load Tests**: MCP service performance
- **Edge Cases**: Malformed emails, encoding issues

### Local Development

```yaml
# docker-compose.yml
version: '3.8'
services:
  minio:
    image: minio/minio
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data
    
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
```

## Performance Requirements

### Phase 1 (Backfill)
- **Target**: Process 70GB overnight (~8 hours)
- **Rate**: 1000-2000 emails/second per worker
- **Memory**: 30GB available (WSL environment)
- **Parallelism**: 2-3 .tgz files simultaneously

### Phase 2 (Sync)
- **Latency**: 5-30 seconds from Gmail receipt
- **Volume**: ~1000 emails/day average
- **Polling**: 30-second intervals

### MCP Service
- **Query Response**: <500ms for metadata, <2s for full content
- **Concurrency**: 10-20 simultaneous connections
- **Connection Pool**: 5-10 SQLite connections

## Future Scalability

### Horizontal Scaling Preparation
- Event streaming architecture ready
- Partitioned Parquet files by date
- Stateless service design
- Health checks for load balancers

### Migration Paths
- SQLite → PostgreSQL for metadata
- Single-node → Kubernetes deployment
- Local storage → Distributed file system
- Bloom filter → Redis for dedup cache

## Development Workflow

### Setup
```bash
# Install dependencies
uv venv
uv pip install -e ".[dev]"

# Run tests
pytest

# Start local environment
docker-compose up -d

# Run bulk loader
postmark-backfill gmail-export-1.tgz
```

### Release Process
- Semantic versioning (major.minor.patch)
- Tagged Docker images
- Automated changelog generation
- Database migration scripts versioned separately

### Documentation
- Architecture Decision Records (ADRs)
- API documentation (OpenAPI spec)
- Operational runbook
- Performance tuning guide

## Implementation Notes

### Technology Stack
- **Language**: Python 3.10+
- **Package Manager**: uv
- **Database**: SQLite + Apache Parquet
- **Object Storage**: S3-compatible (AWS S3, MinIO)
- **Metrics**: Prometheus
- **Container**: Docker (Kubernetes-ready)

### Key Libraries
- **Email Parsing**: python-emailparser, charset-normalizer
- **Parquet**: pyarrow, fastparquet
- **S3**: boto3
- **Fuzzy Matching**: python-Levenshtein
- **MCP**: (implementation TBD based on protocol spec)
- **Metrics**: prometheus-client

### Coding Standards
- Type hints for all functions
- Comprehensive docstrings
- Black formatting
- isort import ordering
- mypy static type checking
- 90% test coverage minimum

This specification provides a complete blueprint for implementing the postmark email datastore. The modular design allows for incremental development while maintaining flexibility for future enhancements.