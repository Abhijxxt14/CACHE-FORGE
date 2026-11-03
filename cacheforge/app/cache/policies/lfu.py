"""LFU (Least Frequently Used) eviction policy.

Evicts the key with the lowest access count; ties are broken by least-recent
access (i.e., among equally-frequent keys the one accessed longest ago loses).
"""
from __future__ import annotations

import heapq
from typing import TYPE_CHECKING

from cacheforge.app.cache.policies.base import EvictionPolicy

if TYPE_CHECKING:
    from cacheforge.app.cache.base import CacheEntry


class LFUPolicy(EvictionPolicy):
    """Least Frequently Used eviction policy with LRU tie-breaking.

    Internal state:
      - ``_freq``:         dict[key, access_count]
      - ``_last_access``:  dict[key, last_accessed_timestamp]

    Eviction candidate selection is O(n) via ``min()``; for the expected
    cache sizes this is acceptable and keeps the implementation simple.
    """

    def __init__(self) -> None:
        self._freq: dict[str, int] = {}
        self._last_access: dict[str, float] = {}

    def on_set(self, entry: "CacheEntry") -> None:
        """Register a new entry or refresh its metadata after an update."""
        key = entry.key
        if key not in self._freq:
            self._freq[key] = entry.access_count
        # On update (key exists) we don't reset the frequency counter —
        # keep accumulated hits.
        self._last_access[key] = entry.last_accessed

    def on_access(self, entry: "CacheEntry") -> None:
        """Increment frequency counter and refresh last-access timestamp."""
        key = entry.key
        self._freq[key] = self._freq.get(key, 0) + 1
        self._last_access[key] = entry.last_accessed

    def on_delete(self, key: str) -> None:
        """Remove the key from all internal structures."""
        self._freq.pop(key, None)
        self._last_access.pop(key, None)

    def evict_candidates(self, n: int = 1) -> list[str]:
        """Return up to *n* keys with the lowest (freq, last_access) score."""
        if not self._freq:
            return []
        # Sort: primary = freq ascending, secondary = last_access ascending
        candidates = sorted(
            self._freq.keys(),
            key=lambda k: (self._freq[k], self._last_access.get(k, 0.0)),
        )
        return candidates[:n]

    def clear(self) -> None:
        self._freq.clear()
        self._last_access.clear()

