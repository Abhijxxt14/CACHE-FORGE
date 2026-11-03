"""LRU (Least Recently Used) eviction policy.

Uses an ``OrderedDict`` to maintain insertion/access order in O(1).
The entry at the *front* (oldest access) is evicted first.
"""
from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from cacheforge.app.cache.policies.base import EvictionPolicy

if TYPE_CHECKING:
    from cacheforge.app.cache.base import CacheEntry


class LRUPolicy(EvictionPolicy):
    """Least Recently Used eviction policy.

    The key that was accessed *least recently* is evicted first.
    Access order is updated on every cache hit (``on_access``).
    """

    def __init__(self) -> None:
        # Maps key -> last_accessed timestamp; order = LRU to MRU
        self._order: OrderedDict[str, float] = OrderedDict()

    def on_set(self, entry: "CacheEntry") -> None:
        """Register or refresh the entry in LRU order."""
        key = entry.key
        if key in self._order:
            self._order.move_to_end(key)
        else:
            self._order[key] = entry.last_accessed
        # Ensure the stored timestamp is up to date
        self._order[key] = entry.last_accessed

    def on_access(self, entry: "CacheEntry") -> None:
        """Move the accessed entry to the most-recently-used end."""
        key = entry.key
        if key in self._order:
            self._order.move_to_end(key)
            self._order[key] = entry.last_accessed

    def on_delete(self, key: str) -> None:
        """Remove the key from the ordering structure."""
        self._order.pop(key, None)

    def evict_candidates(self, n: int = 1) -> list[str]:
        """Return the *n* least-recently-used keys."""
        return list(self._order.keys())[:n]

    def clear(self) -> None:
        self._order.clear()

