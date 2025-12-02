"""Email parsing and processing utilities."""

from .dedup import BloomFilter, should_store
from .parser import parse_email

__all__ = ["parse_email", "BloomFilter", "should_store"]
