"""Eviction policy base class and factory."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cacheforge.app.cache.base import CacheEntry


class EvictionPolicy(ABC):
    """Abstract base for all eviction policies.

    An eviction policy decides the *order* in which keys should be evicted when
    the cache exceeds its limits.  It maintains an internal ordering structure
    that is updated on every ``on_set`` and ``on_access`` call.

    All methods are synchronous — they manipulate in-process data structures.
    The Redis backend wraps equivalent logic in Lua scripts; this Python
    implementation is used by ``InMemoryBackend`` and as the reference for
    correctness testing.
    """

    @abstractmethod
    def on_set(self, entry: "CacheEntry") -> None:
        """Called when a new or updated entry is written to the cache."""

    @abstractmethod
    def on_access(self, entry: "CacheEntry") -> None:
        """Called when an existing entry is read from the cache (cache hit)."""

    @abstractmethod
    def on_delete(self, key: str) -> None:
        """Called when an entry is explicitly removed from the cache."""

    @abstractmethod
    def evict_candidates(self, n: int = 1) -> list[str]:
        """Return up to *n* keys that should be evicted next, in eviction order.

        The keys are returned but NOT removed from the policy's internal state —
        the backend must call :meth:`on_delete` after actually deleting each key.

        Args:
            n: Maximum number of candidates to return.

        Returns:
            List of keys, sorted from *highest eviction priority* (evict first)
            to lowest.
        """

    @abstractmethod
    def clear(self) -> None:
        """Reset all internal state (called on :meth:`CacheBackend.flush`)."""

