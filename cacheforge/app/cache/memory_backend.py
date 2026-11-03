"""In-memory cache backend.

Single-process implementation backed by a plain Python dict.  Used for:

  - Unit tests (no external services required).
  - The reference implementation against which Redis behaviour is verified.

Features:
  - Lazy expiry on ``get`` + optional background sweeper task.
  - Pluggable eviction policy (LRU / LFU / FIFO).
  - Hard limits on ``max_entries`` and ``max_memory_bytes``.
  - Monotonic per-key version counters (separate from the data store so they
    survive entry deletion).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from cacheforge.app.cache.base import CacheBackend, CacheEntry
from cacheforge.app.cache.policies.base import EvictionPolicy

logger = logging.getLogger(__name__)


class InMemoryBackend(CacheBackend):
    """Thread-safe (asyncio-safe) in-memory cache backend.

    Args:
        policy:           Eviction policy instance (LRU / LFU / FIFO).
        max_entries:      Maximum number of entries (0 = unlimited).
        max_memory_bytes: Soft memory limit in bytes (0 = unlimited).
        sweep_interval:   Seconds between background expiry sweeps (0 = disabled).
    """

    def __init__(
        self,
        policy: EvictionPolicy,
        max_entries: int = 0,
        max_memory_bytes: int = 0,
        sweep_interval: float = 60.0,
    ) -> None:
        self._store: dict[str, CacheEntry] = {}
        self._versions: dict[str, int] = {}
        self._policy = policy
        self._max_entries = max_entries
        self._max_memory_bytes = max_memory_bytes
        self._sweep_interval = sweep_interval
        self._total_bytes: int = 0
        self._sweeper_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start_sweeper(self) -> None:
        """Launch the background expiry sweeper coroutine."""
        if self._sweep_interval > 0 and self._sweeper_task is None:
            self._sweeper_task = asyncio.ensure_future(self._sweep_loop())

    async def stop_sweeper(self) -> None:
        """Cancel the background sweeper if it is running."""
        if self._sweeper_task is not None:
            self._sweeper_task.cancel()
            try:
                await self._sweeper_task
            except asyncio.CancelledError:
                pass
            self._sweeper_task = None

    async def _sweep_loop(self) -> None:
        """Periodically remove expired entries."""
        while True:
            await asyncio.sleep(self._sweep_interval)
            await self._sweep_expired()

    async def _sweep_expired(self) -> int:
        """Remove all expired entries.  Returns the count removed."""
        now = time.time()
        expired_keys = [k for k, e in self._store.items() if e.is_expired(now)]
        for key in expired_keys:
            self._remove(key)
        if expired_keys:
            logger.debug("Sweeper removed %d expired entries.", len(expired_keys))
        return len(expired_keys)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _remove(self, key: str) -> Optional[CacheEntry]:
        """Remove an entry from the store + policy; update byte counter."""
        entry = self._store.pop(key, None)
        if entry is not None:
            self._total_bytes -= entry.size_bytes
            self._policy.on_delete(key)
        return entry

    def _evict_to_fit(self, incoming_bytes: int) -> None:
        """Evict entries until limits would not be exceeded by the incoming entry."""
        while self._store:
            over_entries = (
                self._max_entries > 0
                and len(self._store) >= self._max_entries
            )
            over_memory = (
                self._max_memory_bytes > 0
                and self._total_bytes + incoming_bytes > self._max_memory_bytes
            )
            if not over_entries and not over_memory:
                break
            candidates = self._policy.evict_candidates(1)
            if not candidates:
                break
            victim = candidates[0]
            logger.debug("Evicting key=%r (policy=%s)", victim, type(self._policy).__name__)
            self._remove(victim)

    # ── CacheBackend interface ────────────────────────────────────────────

    async def get(self, key: str) -> Optional[CacheEntry]:
        """Return the entry for *key* or ``None`` if absent / expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.is_expired():
            self._remove(key)
            return None
        # Update access metadata
        entry.touch()
        self._policy.on_access(entry)
        return entry

    async def set(self, entry: CacheEntry) -> None:
        """Write *entry* to the store, evicting if necessary."""
        key = entry.key
        # If this key already exists, remove it first (to correctly update counters)
        if key in self._store:
            self._remove(key)
        # Evict until the new entry fits
        self._evict_to_fit(entry.size_bytes)
        # Store
        self._store[key] = entry
        self._total_bytes += entry.size_bytes
        self._policy.on_set(entry)

    async def delete(self, key: str) -> bool:
        """Delete the entry for *key*.  Returns True if it existed."""
        entry = self._remove(key)
        return entry is not None

    async def exists(self, key: str) -> bool:
        """Return True if *key* is present and not expired."""
        return await self.get(key) is not None

    async def delete_by_prefix(self, prefix: str) -> int:
        """Delete all keys starting with *prefix*."""
        victims = [k for k in list(self._store.keys()) if k.startswith(prefix)]
        for key in victims:
            self._remove(key)
        return len(victims)

    async def flush(self) -> int:
        """Remove all entries from the backend."""
        count = len(self._store)
        self._store.clear()
        self._total_bytes = 0
        self._policy.clear()
        return count

    # ── Versioning ────────────────────────────────────────────────────────

    async def increment_version(self, key: str) -> int:
        """Atomically increment and return the version counter for *key*."""
        self._versions[key] = self._versions.get(key, 0) + 1
        return self._versions[key]

    async def get_version(self, key: str) -> int:
        """Return the current version for *key* (0 if never set)."""
        return self._versions.get(key, 0)

    # ── Introspection ─────────────────────────────────────────────────────

    async def size(self) -> int:
        """Return the number of live (non-expired) entries."""
        now = time.time()
        return sum(1 for e in self._store.values() if not e.is_expired(now))

    async def memory_bytes(self) -> int:
        """Return the running byte counter for live entries."""
        return self._total_bytes

    async def info(self) -> dict[str, Any]:
        return {
            "backend": "memory",
            "entries": await self.size(),
            "memory_bytes": await self.memory_bytes(),
            "max_entries": self._max_entries,
            "max_memory_bytes": self._max_memory_bytes,
            "policy": type(self._policy).__name__,
        }

