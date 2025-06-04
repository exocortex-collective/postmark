from typing import List
from dataclasses import dataclass
import math
import random
import string
from Levenshtein import ratio as lev_ratio


class BloomFilter:
    def __init__(self, capacity: int, error_rate: float) -> None:
        self.capacity = capacity
        self.error_rate = error_rate
        # For simplicity, pick a bit array size derived from false-positive rate
        # (Equivalent to: m = -capacity * ln(error_rate) / (ln(2)^2))
        self.size = int(-capacity * math.log(error_rate, 2) / (math.log(2)))
        self.bit_array = [False] * self.size
        # Number of hash functions
        self.num_hashes = max(1, int(self.size / capacity * math.log(2)))

    def _hashes(self, item: str) -> List[int]:
        # Generate self.num_hashes different integer hashes
        out = []
        random.seed(0)  # We only use seed to change salt, must be consistent for all items
        for i in range(self.num_hashes):
            salt = ''.join(random.choices(string.ascii_letters, k=5))
            h = abs(hash(item + salt)) % self.size
            out.append(h)
        return out

    def add(self, item: str) -> None:
        for h in self._hashes(item):
            self.bit_array[h] = True

    def __contains__(self, item: str) -> bool:
        # If any hash bit is not set, item is definitely not in filter
        for h in self._hashes(item):
            if not self.bit_array[h]:
                return False
        return True


def fuzzy_equal(a: str, b: str, threshold: float = 0.95) -> bool:
    return lev_ratio(a, b) >= threshold


def should_store(email, bloom: BloomFilter, recent_bodies: list[str], threshold: float = 0.95) -> bool:
    # If there's a message_id, we check Bloom first
    if email.message_id and (email.message_id in bloom):
        return False
    # Then check fuzzy match against recently seen bodies
    for rb in recent_bodies:
        if fuzzy_equal(email.body_text, rb, threshold=threshold):
            return False
    return True
