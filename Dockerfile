FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy everything needed for build
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

# Install dependencies (includes building the package)
RUN uv sync --frozen --no-dev

# Create data directory for DuckDB
RUN mkdir -p /app/data

# Secrets will be mounted at runtime
VOLUME /app/secrets
VOLUME /app/data

# Run the gmail-sync service
CMD ["uv", "run", "python", "-m", "postmark.services.gmail_sync"]
