"""FIFO (First In, First Out) eviction policy.

Evicts keys in insertion order, ignoring access patterns.
"""
from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from cacheforge.app.cache.policies.base import EvictionPolicy

if TYPE_CHECKING:
    from cacheforge.app.cache.base import CacheEntry


class FIFOPolicy(EvictionPolicy):
    """First In, First Out eviction policy.

    The key that has been in the cache the *longest* is evicted first,
    regardless of how frequently or recently it was accessed.
    """

    def __init__(self) -> None:
        # Maps key -> created_at timestamp; order = oldest to newest
        self._insertion_order: OrderedDict[str, float] = OrderedDict()

    def on_set(self, entry: "CacheEntry") -> None:
        """Register a new entry.  Existing entries keep their original position."""
        key = entry.key
        if key not in self._insertion_order:
            self._insertion_order[key] = entry.created_at
        # On update, FIFO keeps the *original* insertion timestamp.

    def on_access(self, entry: "CacheEntry") -> None:
        """No-op — FIFO does not reorder on access."""

    def on_delete(self, key: str) -> None:
        """Remove the key from the insertion order."""
        self._insertion_order.pop(key, None)

    def evict_candidates(self, n: int = 1) -> list[str]:
        """Return the *n* oldest-inserted keys."""
        return list(self._insertion_order.keys())[:n]

    def clear(self) -> None:
        self._insertion_order.clear()

