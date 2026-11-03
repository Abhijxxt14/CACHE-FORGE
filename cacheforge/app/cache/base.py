"""Abstract base classes for the CacheForge cache layer.

Defines:
  - ``CacheEntry``: the envelope stored alongside every cached value.
  - ``CacheBackend``: the ABC that all backend implementations must satisfy.
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import BaseModel, Field


# ── Cache entry ───────────────────────────────────────────────────────────────

class CacheEntry(BaseModel):
    """Metadata envelope stored with every cached value.

    Attributes:
        key:          The namespaced cache key.
        value:        The serialized value (JSON wire string).
        ttl:          Time-to-live in seconds (None = no expiry).
        created_at:   Unix timestamp when the entry was first written.
        expires_at:   Unix timestamp when the entry expires (None = never).
        last_accessed: Unix timestamp of the most recent cache hit.
        access_count: Number of times this entry has been accessed (for LFU).
        version:      Monotonically increasing generation counter.  Incremented
                      on invalidation; used to prevent stale repopulation.
        is_negative:  True if this entry caches a "not found" result.
        size_bytes:   Estimated memory size of this entry in bytes.
    """

    key: str
    value: str  # serialized via cacheforge.app.cache.serializer
    ttl: Optional[int] = None
    created_at: float = Field(default_factory=time.time)
    expires_at: Optional[float] = None
    last_accessed: float = Field(default_factory=time.time)
    access_count: int = 0
    version: int = 0
    is_negative: bool = False
    size_bytes: int = 0

    def is_expired(self, now: Optional[float] = None) -> bool:
        """Return True if the entry has passed its expiry timestamp."""
        if self.expires_at is None:
            return False
        return (now if now is not None else time.time()) >= self.expires_at

    def touch(self, now: Optional[float] = None) -> None:
        """Update access metadata in-place (for LRU/LFU tracking)."""
        self.last_accessed = now if now is not None else time.time()
        self.access_count += 1


# ── Backend ABC ───────────────────────────────────────────────────────────────

class CacheBackend(ABC):
    """Abstract base class for all CacheForge storage backends.

    All methods are async so implementations may use I/O without blocking.
    """

    # ── Core CRUD ─────────────────────────────────────────────────────────

    @abstractmethod
    async def get(self, key: str) -> Optional[CacheEntry]:
        """Return the ``CacheEntry`` for *key*, or ``None`` if absent / expired."""

    @abstractmethod
    async def set(self, entry: CacheEntry) -> None:
        """Store *entry*, overwriting any existing entry for the same key.

        Implementations must apply eviction if the write would exceed configured
        limits before inserting the new entry.
        """

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete the entry for *key*.

        Returns:
            True if an entry existed and was removed, False otherwise.
        """

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if *key* is present and not expired."""

    # ── Bulk / prefix operations ──────────────────────────────────────────

    @abstractmethod
    async def delete_by_prefix(self, prefix: str) -> int:
        """Delete all keys that begin with *prefix*.

        Returns:
            Number of keys deleted.
        """

    @abstractmethod
    async def flush(self) -> int:
        """Remove all entries from this backend.

        Returns:
            Number of keys deleted.
        """

    # ── Versioning ────────────────────────────────────────────────────────

    @abstractmethod
    async def increment_version(self, key: str) -> int:
        """Atomically increment and return the version counter for *key*.

        The version counter must persist even after the data entry is deleted,
        so that in-flight loaders can detect that an invalidation occurred.
        """

    @abstractmethod
    async def get_version(self, key: str) -> int:
        """Return the current version counter for *key* (0 if never set)."""

    # ── Introspection ─────────────────────────────────────────────────────

    @abstractmethod
    async def size(self) -> int:
        """Return the number of live (non-expired) entries in the backend."""

    @abstractmethod
    async def memory_bytes(self) -> int:
        """Return an estimate of memory used by cache entries in bytes."""

    @abstractmethod
    async def info(self) -> dict[str, Any]:
        """Return backend-specific diagnostic information."""

