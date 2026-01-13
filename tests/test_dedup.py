from datetime import datetime, timezone

from postmark.email.dedup import BloomFilter, fuzzy_equal, should_store
from postmark.email.parser import Email


def test_bloom_filter() -> None:
    bf = BloomFilter(capacity=500, error_rate=0.01)
    items = [f"test{i}" for i in range(100)]
    for it in items:
        bf.add(it)
    # Check no false negatives
    for it in items:
        assert it in bf


def test_fuzzy_equal() -> None:
    text_a = "hello world"
    text_b = "hello  world"
    assert fuzzy_equal(text_a, text_b, threshold=0.9) is True
    assert fuzzy_equal(text_a, text_b, threshold=0.99) is False


def test_should_store_new_email() -> None:
    """Test should_store returns True for new email."""
    bloom = BloomFilter(capacity=100, error_rate=0.01)
    email = Email(
        uuid="test-uuid",
        message_id="<new@example.com>",
        date=datetime.now(timezone.utc),
        subject="Test",
        from_addr="sender@example.com",
        body_text="This is a new email body",
        body_html="",
        raw_msg=b"",
    )
    assert should_store(email, bloom, []) is True


def test_should_store_duplicate_message_id() -> None:
    """Test should_store returns False when message_id is in bloom filter."""
    bloom = BloomFilter(capacity=100, error_rate=0.01)
    message_id = "<duplicate@example.com>"
    bloom.add(message_id)

    email = Email(
        uuid="test-uuid",
        message_id=message_id,
        date=datetime.now(timezone.utc),
        subject="Test",
        from_addr="sender@example.com",
        body_text="Email body",
        body_html="",
        raw_msg=b"",
    )
    assert should_store(email, bloom, []) is False


def test_should_store_fuzzy_duplicate_body() -> None:
    """Test should_store returns False when body is similar to recent body."""
    bloom = BloomFilter(capacity=100, error_rate=0.01)
    recent_body = "This is the original email body"
    recent_bodies = [recent_body]

    # Email with very similar body (just extra whitespace)
    email = Email(
        uuid="test-uuid",
        message_id="<unique@example.com>",
        date=datetime.now(timezone.utc),
        subject="Test",
        from_addr="sender@example.com",
        body_text="This is  the  original email body",  # Extra spaces
        body_html="",
        raw_msg=b"",
    )
    assert should_store(email, bloom, recent_bodies) is False


def test_should_store_different_body() -> None:
    """Test should_store returns True when body is different from recent bodies."""
    bloom = BloomFilter(capacity=100, error_rate=0.01)
    recent_bodies = ["Completely different email content"]

    email = Email(
        uuid="test-uuid",
        message_id="<new@example.com>",
        date=datetime.now(timezone.utc),
        subject="Test",
        from_addr="sender@example.com",
        body_text="This is a totally new email body",
        body_html="",
        raw_msg=b"",
    )
    assert should_store(email, bloom, recent_bodies) is True
