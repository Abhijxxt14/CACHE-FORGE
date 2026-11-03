"""Key generators for load testing: Zipfian (hot keys) and Uniform."""
from __future__ import annotations

import bisect
import random


class ZipfianGenerator:
    """Generates integer keys in range [1, n] according to a Zipf distribution.

    Args:
        n: Total number of keys in the dataset.
        s: Exponent parameter (s > 0). Typically ~1.0 - 1.2 for web workloads.
    """

    def __init__(self, n: int = 1000, s: float = 1.1) -> None:
        self.n = max(1, n)
        self.s = s
        # Precompute CDF
        weights = [1.0 / (i ** s) for i in range(1, self.n + 1)]
        total = sum(weights)
        probs = [w / total for w in weights]
        self._cdf: list[float] = []
        acc = 0.0
        for p in probs:
            acc += p
            self._cdf.append(acc)
        self._cdf[-1] = 1.0  # Avoid roundoff errors

    def next_key(self) -> int:
        """Return a 1-based item key according to Zipfian distribution."""
        u = random.random()
        idx = bisect.bisect_left(self._cdf, u)
        return idx + 1


class UniformGenerator:
    """Generates integer keys uniformly in range [1, n]."""

    def __init__(self, n: int = 1000) -> None:
        self.n = max(1, n)

    def next_key(self) -> int:
        return random.randint(1, self.n)

