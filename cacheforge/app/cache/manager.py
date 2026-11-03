"""CacheManager: cache-aside logic, stampede protection, and consistency.

This is the central orchestrator that the API layer calls.  It:

  1. Implements the cache-aside pattern (get_or_load).
  2. Provides stampede protection: per-key asyncio lock (in-process) + Redis
     distributed lock (cross-instance) so exactly one instance loads from DB.
  3. Enforces version tokens to prevent stale repopulation after invalidation.
  4. Caches negative ("not found") results to avoid repeated DB hits.
  5. Gracefully degrades to direct DB reads when Redis is unavailable.
  6. Applies configurable TTL jitter to avoid synchronized mass expiry.

Design assumptions (documented in README):
  - The brief window between a DB commit and the subsequent cache invalidation
    is a known, acceptable inconsistency window (~sub-millisecond in practice).
  - Per-process asyncio locks do NOT protect against stampede across instances;
    the Redis distributed lock handles cross-instance protection.
  - If the Redis lock TTL expires before the loader finishes, multiple instances
    may load from DB concurrently, but only one write will succeed (version check).
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from typing import Any, Callable, Coroutine, Optional

from cacheforge.app.cache.base import CacheBackend, CacheEntry
from cacheforge.app.cache.redis_backend import RedisBackend
from cacheforge.app.cache.serializer import deserialize, estimate_size, serialize
from cacheforge.app.config import settings
from cacheforge.app.metrics.counters import metrics

logger = logging.getLogger(__name__)

# Type alias for the DB loader callback
Loader = Callable[[], Coroutine[Any, Any, Any]]


class CacheManager:
    """High-level cache-aside manager.

    Args:
        backend:    The cache storage backend (Redis in production, in-memory
                    for unit tests).
        namespace:  Key namespace; defaults to ``settings.cache_namespace``.
        default_ttl: Default TTL in seconds; defaults to ``settings.cache_default_ttl``.
        negative_ttl: TTL for negative-cache entries; defaults to ``settings.cache_negative_ttl``.
        ttl_jitter: Max random jitter (seconds) added to every TTL.
        stampede_timeout: Seconds a waiter will wait before falling back to a
                    direct DB read.
        lock_ttl_ms: Redis distributed lock TTL (ms).
    """

    def __init__(
        self,
        backend: CacheBackend,
        namespace: str = "",
        default_ttl: int = 0,
        negative_ttl: int = 0,
        ttl_jitter: int = 0,
        stampede_timeout: float = 0.0,
        lock_ttl_ms: int = 0,
    ) -> None:
        self._backend = backend
        self._ns = namespace or settings.cache_namespace
        self._default_ttl = default_ttl or settings.cache_default_ttl
        self._negative_ttl = negative_ttl or settings.cache_negative_ttl
        self._ttl_jitter = ttl_jitter if ttl_jitter >= 0 else settings.cache_ttl_jitter
        self._stampede_timeout = stampede_timeout or settings.cache_stampede_timeout
        self._lock_ttl_ms = lock_ttl_ms or settings.cache_lock_ttl
        # Per-key asyncio locks for in-process stampede protection
        self._in_flight: dict[str, asyncio.Event] = {}
        self._in_flight_lock = asyncio.Lock()

    # ── Key management ────────────────────────────────────────────────────

    def make_key(self, raw_key: str, namespace: Optional[str] = None) -> str:
        """Build a namespaced cache key."""
        ns = namespace or self._ns
        return f"{ns}:{raw_key}"

    def _jitter(self, ttl: int) -> int:
        """Apply random jitter to a TTL value."""
        if self._ttl_jitter <= 0:
            return ttl
        return ttl + random.randint(0, self._ttl_jitter)

    # ── Core CRUD (thin wrappers that add key namespacing) ─────────────────

    async def get(self, key: str) -> Optional[Any]:
        """Return the value for *key* (deserialized) or None if not cached."""
        entry = await self._backend.get(key)
        if entry is None:
            metrics.record_miss(key)
            return None
        if entry.is_negative:
            metrics.record_negative_hit(key)
            return None  # Cached "not found"
        metrics.record_hit(key)
        return deserialize(entry.value)

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        is_negative: bool = False,
        version: int = -1,
    ) -> None:
        """Serialize and store *value* under *key*.

        Args:
            key:         Full namespaced cache key.
            value:       Python value to cache.
            ttl:         Per-key TTL (seconds); falls back to default.
            is_negative: True to cache a "not found" sentinel.
            version:     Expected version token (-1 = skip check).
        """
        effective_ttl = self._jitter(ttl if ttl is not None else (
            self._negative_ttl if is_negative else self._default_ttl
        ))
        raw = serialize(value)
        size = estimate_size(key, value)
        now = time.time()
        entry = CacheEntry(
            key=key,
            value=raw,
            ttl=effective_ttl,
            created_at=now,
            expires_at=now + effective_ttl,
            last_accessed=now,
            access_count=0,
            version=version,
            is_negative=is_negative,
            size_bytes=size,
        )
        await self._backend.set(entry)

    async def delete(self, key: str) -> bool:
        """Delete *key* and increment its version counter."""
        await self._backend.increment_version(key)
        return await self._backend.delete(key)

    async def exists(self, key: str) -> bool:
        return await self._backend.exists(key)

    async def invalidate_prefix(self, prefix: str) -> int:
        return await self._backend.delete_by_prefix(prefix)

    async def flush(self) -> int:
        return await self._backend.flush()

    # ── Cache-aside: get_or_load ───────────────────────────────────────────

    async def get_or_load(
        self,
        key: str,
        loader: Loader,
        ttl: Optional[int] = None,
        negative_ttl: Optional[int] = None,
    ) -> Any:
        """Cache-aside: return cached value or call *loader* on a miss.

        Stampede protection
        -------------------
        On a cache miss the first coroutine to arrive acquires:
          1. An in-process asyncio.Event (prevents N concurrent requests in
             the same process from all calling loader simultaneously).
          2. A Redis distributed lock (prevents concurrent loaders across
             different API instances).

        Waiters poll the in-process event.  If the event is not set within
        ``stampede_timeout`` seconds, the waiter falls back to a direct DB read
        (without caching the result — metrics count this as a stampede fallback).

        Stale-repopulation guard
        ------------------------
        Before calling the DB loader, the caller captures the current version
        token.  The subsequent ``set`` only succeeds if the version is unchanged
        (i.e., no invalidation occurred while the DB was being queried).  If the
        token has changed the loader returns its fresh value but does NOT cache it.

        Args:
            key:          Full namespaced cache key.
            loader:       Async callable that fetches the value from the DB.
            ttl:          Override TTL for positive results.
            negative_ttl: Override TTL for negative (not-found) results.

        Returns:
            The cached or freshly loaded value (None if not found).
        """
        # ── 1. Check cache ────────────────────────────────────────────────
        entry = None
        try:
            entry = await self._backend.get(key)
        except Exception as exc:
            logger.warning("Cache get error for key %r (%s); proceeding to DB", key, exc)
            metrics.record_redis_unavailable()

        if entry is not None:
            if entry.is_negative:
                metrics.record_negative_hit(key)
                return None
            metrics.record_hit(key)
            return deserialize(entry.value)

        metrics.record_miss(key)

        # ── 2. In-process stampede check ──────────────────────────────────
        async with self._in_flight_lock:
            if key in self._in_flight:
                # Another coroutine in THIS process is loading — wait for it
                event = self._in_flight[key]
                is_leader = False
            else:
                event = asyncio.Event()
                self._in_flight[key] = event
                is_leader = True

        if not is_leader:
            # Wait for the leader to populate the cache
            try:
                await asyncio.wait_for(event.wait(), timeout=self._stampede_timeout)
                # Leader is done — check cache again
                entry = await self._backend.get(key)
                if entry is not None:
                    return None if entry.is_negative else deserialize(entry.value)
                # Cache still empty (leader may have failed) — fall through to direct load
                metrics.record_stampede_fallback(key)
            except asyncio.TimeoutError:
                logger.warning("Stampede timeout waiting for key %r; falling back to DB", key)
                metrics.record_stampede_fallback(key)
            # Fall back to a direct DB read (not cached)
            return await loader()

        # ── 3. Leader: capture version and acquire distributed lock ───────
        version_before = 0
        try:
            version_before = await self._backend.get_version(key)
        except Exception as exc:
            logger.warning("Cache get_version error for key %r: %s", key, exc)

        # Try Redis distributed lock (cross-instance stampede protection)
        lock_token = str(uuid.uuid4())
        lock_acquired = False
        if isinstance(self._backend, RedisBackend):
            try:
                lock_acquired = await self._backend.acquire_lock(
                    key, lock_token, self._lock_ttl_ms
                )
                if not lock_acquired:
                    # Another instance holds the lock — fall back to direct DB
                    metrics.record_stampede_fallback(key)
                    return await loader()
            except Exception as exc:
                logger.warning("Redis lock acquire failed for %r: %s", key, exc)
                # Degraded — continue without distributed lock

        # ── 4. Load from DB ───────────────────────────────────────────────
        value = None
        db_exception: Optional[Exception] = None
        try:
            t0 = time.perf_counter()
            value = await loader()
            metrics.record_db_query(time.perf_counter() - t0)
        except Exception as exc:
            db_exception = exc
            logger.error("Loader failed for key %r: %s", key, exc)

        # ── 5. Populate cache (version-guarded) ───────────────────────────
        if db_exception is None:
            version_now = await self._backend.get_version(key)
            if version_now == version_before:
                # Version unchanged: safe to cache
                effective_ttl = ttl if ttl is not None else self._default_ttl
                eff_neg_ttl = negative_ttl if negative_ttl is not None else self._negative_ttl
                try:
                    if value is None:
                        await self.set(
                            key, None,
                            ttl=eff_neg_ttl,
                            is_negative=True,
                            version=version_before,
                        )
                    else:
                        await self.set(
                            key, value,
                            ttl=effective_ttl,
                            is_negative=False,
                            version=version_before,
                        )
                except Exception as exc:
                    logger.warning("Cache set failed for key %r: %s", key, exc)
            else:
                logger.debug(
                    "Stale repopulation avoided for key %r (version %d -> %d)",
                    key, version_before, version_now,
                )
                metrics.record_stale_prevented(key)

        # ── 6. Release lock and signal waiters ────────────────────────────
        if lock_acquired and isinstance(self._backend, RedisBackend):
            try:
                await self._backend.release_lock(key, lock_token)
            except Exception as exc:
                logger.warning("Redis lock release failed for %r: %s", key, exc)

        async with self._in_flight_lock:
            self._in_flight.pop(key, None)
        event.set()

        if db_exception is not None:
            raise db_exception
        return value

    # ── Consistency helpers ───────────────────────────────────────────────

    async def invalidate_on_update(self, key: str) -> None:
        """Invalidate the cache entry after a DB update (cache-aside consistency).

        Increments the version counter then deletes the key so any in-flight
        loader will see a changed version and refuse to cache its stale result.
        """
        await self.delete(key)

    # ── Backend info ──────────────────────────────────────────────────────

    async def stats(self) -> dict[str, Any]:
        """Return cache statistics for the /cache/stats endpoint."""
        backend_info = await self._backend.info()
        return {
            **backend_info,
            **metrics.snapshot(),
        }

