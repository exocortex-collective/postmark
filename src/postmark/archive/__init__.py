"""Archived modules from the Gmail takeout-based approach.

These modules implement batch processing of Gmail takeout exports (.mbox.gz files)
into columnar storage (Parquet). This approach is preserved for potential future
use with historical email imports, but the primary architecture is now focused
on real-time Gmail API integration.

Modules:
- storage/columnar.py: Parquet I/O with monthly partitioning
- cli/backfill.py: CLI for batch ingestion of mbox files
- email/processing.py: Pipeline for takeout email processing
"""
