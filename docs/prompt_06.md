**Context**
- Parsed emails are available.
- We must prevent duplicates using Bloom filter + fuzzy match.

**Task**
1. Add `postmark/email/dedup.py` implementing:
   - `BloomFilter(capacity: int, error_rate: float)`
     * `add` and `__contains__`.
   - `fuzzy_equal(a: str, b: str, threshold: float = 0.95) -> bool` using Levenshtein ratio.
2. Expose `should_store(email, bloom, recent_bodies)` that:
   - Returns `False` if Message‑ID in bloom or fuzzy match hits.

**Tests**
- Bloom: add 100 strings, assert no false negatives.
- Fuzzy: “hello world” vs “hello  world” == True at 0.9, False at 0.99.

Write the code and the tests.
