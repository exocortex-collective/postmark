# Postmark Email DataStore

A high-performance email datastore designed to ingest, store, and expose email data for automation tasks and AI agents.

## Features

- Process and store Gmail exports 
- Maintain real-time synchronization
- Provide fast, flexible access to email data
- Support future enrichment workflows

## Development

### Setup

```bash
# Create and activate virtual environment with uv
uv venv
source .venv/bin/activate

# Install development dependencies
uv run pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Project Structure

```
src/
  postmark/
    __init__.py
    storage/      # SQLite, Parquet, S3 operations
    email/        # RFC822 parsing, normalization
    sync/         # Gmail API, IMAP synchronization
    cli/          # Command-line interfaces
```

### Testing

```bash
# Run tests
pytest

# Run pre-commit hooks
pre-commit run --all-files
```

## License

MIT
