from postmark.email.dedup import BloomFilter, fuzzy_equal


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
