"""Redis-backed cache backend.

Uses redis-py async client with Lua scripts for all atomic operations so that
write, eviction-metadata update, and memory accounting are a single round-trip.

Eviction is implemented using Redis sorted sets:
  - LRU / FIFO: score = Unix timestamp (ms).  Lower = older = evict first.
  - LFU:        score = (freq * 2^20) | (ts_ms % 2^20).  Lower = rarer/older.

Key layout
----------
  <namespace>:<key>            — the actual cache data (STRING)
  <namespace>:__meta:<policy>  — eviction sorted set (ZSET)
  <namespace>:__mem            — per-key size + running total (HASH)
  <namespace>:__ver            — per-key version counter (HASH)
  <namespace>:__meta:<policy>:freq — LFU frequency counters (HASH)
  <namespace>:__lock:<key>     — distributed stampede lock (STRING, SET NX)
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

from redis.asyncio import Redis
from redis.exceptions import RedisError

from cacheforge.app.cache.base import CacheBackend, CacheEntry
from cacheforge.app.cache.serializer import serialize, deserialize, estimate_size
from cacheforge.app.config import EvictionPolicyEnum, settings
from cacheforge.app.metrics.counters import metrics

logger = logging.getLogger(__name__)

# Load Lua scripts
_LUA_DIR = Path(__file__).parent / "lua"


def _load_lua(name: str) -> str:
    return (_LUA_DIR / name).read_text(encoding="utf-8")


_SET_ENTRY_LUA = _load_lua("set_entry.lua")
_DELETE_ENTRY_LUA = _load_lua("delete_entry.lua")
_EVICT_VICTIMS_LUA = _load_lua("evict_victims.lua")


class RedisBackend(CacheBackend):
    """Distributed Redis-backed cache backend.

    Args:
        redis:            An asyncio Redis client instance.
        namespace:        Key namespace prefix (default from settings).
        policy:           Eviction policy name.
        max_entries:      0 = unlimited.
        max_memory_bytes: 0 = unlimited.
    """

    def __init__(
        self,
        redis: Redis,
        namespace: str = "",
        policy: EvictionPolicyEnum = EvictionPolicyEnum.lru,
        max_entries: int = 0,
        max_memory_bytes: int = 0,
    ) -> None:
        self._redis = redis
        self._ns = namespace or settings.cache_namespace
        self._policy = policy
        self._max_entries = max_entries
        self._max_memory_bytes = max_memory_bytes

        # Register scripts for sha caching
        self._set_script = self._redis.register_script(_SET_ENTRY_LUA)
        self._delete_script = self._redis.register_script(_DELETE_ENTRY_LUA)
        self._evict_script = self._redis.register_script(_EVICT_VICTIMS_LUA)

    # ── Key helpers ───────────────────────────────────────────────────────

    def _meta_key(self) -> str:
        return f"{self._ns}:__meta:{self._policy.value}"

    def _mem_key(self) -> str:
        return f"{self._ns}:__mem"

    def _ver_key(self) -> str:
        return f"{self._ns}:__ver"

    def _freq_key(self) -> str:
        return f"{self._ns}:__meta:{self._policy.value}:freq" if self._policy == EvictionPolicyEnum.lfu else ""

    def _lock_key(self, data_key: str) -> str:
        return f"{self._ns}:__lock:{data_key}"

    # ── Eviction score ────────────────────────────────────────────────────

    def _score(self, now_ms: Optional[float] = None) -> float:
        """Compute an eviction score for set/update operations.

        LRU / FIFO use the current timestamp (ms).
        LFU score is computed inside the Lua script; we pass 0 as placeholder.
        """
        if self._policy == EvictionPolicyEnum.lfu:
            return 0.0
        return now_ms if now_ms is not None else time.time() * 1000

    # ── Eviction enforcement ──────────────────────────────────────────────

    async def _evict_if_needed(self, incoming_bytes: int) -> None:
        """Evict entries until both limits would not be exceeded."""
        while True:
            count = await self._redis.zcard(self._meta_key())
            total_mem = int(await self._redis.hget(self._mem_key(), "__total") or 0)

            over_entries = self._max_entries > 0 and count >= self._max_entries
            over_memory = self._max_memory_bytes > 0 and (total_mem + incoming_bytes) > self._max_memory_bytes

            if not over_entries and not over_memory:
                break

            n_evict = max(1, count // 10)  # evict up to 10 % at a time
            evicted = await self._evict_script(
                keys=[self._meta_key(), self._mem_key(), self._freq_key()],
                args=[n_evict],
            )
            if not evicted:
                break
            logger.debug("Evicted %d entries from Redis cache.", len(evicted))

    # ── CacheBackend interface ────────────────────────────────────────────

    async def get(self, key: str) -> Optional[CacheEntry]:
        """Return the entry for *key* or None."""
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None

            value = deserialize(raw.decode() if isinstance(raw, bytes) else raw)
            # Reconstruct a CacheEntry from metadata
            entry = CacheEntry(
                key=key,
                value=raw.decode() if isinstance(raw, bytes) else raw,
                last_accessed=time.time(),
            )
            entry.touch()

            # Update LRU / LFU score on hit
            score = self._score()
            await self._set_script(
                keys=[key, self._meta_key(), self._mem_key(), self._ver_key()],
                args=[
                    entry.value,  # keep same payload
                    0,            # 0 = preserve existing TTL (won't re-set)
                    score,
                    entry.size_bytes or estimate_size(key, value),
                    -1,           # -1 = skip version check
                    1 if self._policy == EvictionPolicyEnum.lfu else 0,
                ],
            )
            return entry
        except RedisError as exc:
            logger.warning("Redis GET error for key %r: %s", key, exc)
            metrics.record_redis_unavailable()
            return None

    async def set(self, entry: CacheEntry) -> None:
        """Write *entry* to Redis, evicting if necessary."""
        try:
            await self._evict_if_needed(entry.size_bytes)

            ttl_ms = int(entry.ttl * 1000) if entry.ttl else 0
            score = self._score(time.time() * 1000)

            result = await self._set_script(
                keys=[entry.key, self._meta_key(), self._mem_key(), self._ver_key()],
                args=[
                    entry.value,
                    ttl_ms,
                    score,
                    entry.size_bytes,
                    entry.version,   # expected version (-1 to skip check)
                    1 if self._policy == EvictionPolicyEnum.lfu else 0,
                ],
            )
            if result == 0:
                logger.debug("set() rejected by version check for key %r", entry.key)
        except RedisError as exc:
            logger.warning("Redis SET error for key %r: %s", entry.key, exc)
            metrics.record_redis_unavailable()

    async def delete(self, key: str) -> bool:
        """Delete *key* and return True if it existed."""
        try:
            new_ver = await self._delete_script(
                keys=[key, self._meta_key(), self._mem_key(), self._ver_key(), self._freq_key()],
                args=[],
            )
            return new_ver is not None
        except RedisError as exc:
            logger.warning("Redis DELETE error for key %r: %s", key, exc)
            metrics.record_redis_unavailable()
            return False

    async def exists(self, key: str) -> bool:
        try:
            return bool(await self._redis.exists(key))
        except RedisError as exc:
            logger.warning("Redis EXISTS error: %s", exc)
            metrics.record_redis_unavailable()
            return False

    async def delete_by_prefix(self, prefix: str) -> int:
        """Delete all keys starting with *prefix* using SCAN (never KEYS)."""
        deleted = 0
        try:
            async for key in self._redis.scan_iter(match=f"{prefix}*", count=100):
                key_str = key.decode() if isinstance(key, bytes) else key
                await self.delete(key_str)
                deleted += 1
        except RedisError as exc:
            logger.warning("Redis SCAN/DELETE error for prefix %r: %s", prefix, exc)
            metrics.record_redis_unavailable()
        return deleted

    async def flush(self) -> int:
        """Flush all entries in this namespace using SCAN."""
        deleted = 0
        try:
            # Flush data keys
            async for key in self._redis.scan_iter(match=f"{self._ns}:*", count=100):
                await self._redis.delete(key)
                deleted += 1
        except RedisError as exc:
            logger.warning("Redis FLUSH error: %s", exc)
            metrics.record_redis_unavailable()
        return deleted

    # ── Versioning ────────────────────────────────────────────────────────

    async def increment_version(self, key: str) -> int:
        try:
            return int(await self._redis.hincrby(self._ver_key(), key, 1))
        except RedisError as exc:
            logger.warning("Redis HINCRBY error: %s", exc)
            metrics.record_redis_unavailable()
            return 0

    async def get_version(self, key: str) -> int:
        try:
            val = await self._redis.hget(self._ver_key(), key)
            return int(val) if val is not None else 0
        except RedisError as exc:
            logger.warning("Redis HGET error: %s", exc)
            metrics.record_redis_unavailable()
            return 0

    # ── Distributed lock (stampede protection) ────────────────────────────

    async def acquire_lock(
        self, data_key: str, token: str, ttl_ms: int = 10_000
    ) -> bool:
        """Acquire a distributed lock for *data_key*.

        Args:
            data_key: The cache data key being populated.
            token:    A unique identifier for this lock holder.
            ttl_ms:   Lock expiry in milliseconds.

        Returns:
            True if the lock was acquired, False if another holder owns it.
        """
        lock_key = self._lock_key(data_key)
        result = await self._redis.set(lock_key, token, nx=True, px=ttl_ms)
        return bool(result)

    async def release_lock(self, data_key: str, token: str) -> bool:
        """Release the lock only if *token* matches (safe compare-and-delete via Lua)."""
        lock_key = self._lock_key(data_key)
        lua = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""
        script = self._redis.register_script(lua)
        result = await script(keys=[lock_key], args=[token])
        return bool(result)

    # ── Introspection ─────────────────────────────────────────────────────

    async def size(self) -> int:
        try:
            return await self._redis.zcard(self._meta_key())
        except RedisError:
            return 0

    async def memory_bytes(self) -> int:
        try:
            val = await self._redis.hget(self._mem_key(), "__total")
            return int(val) if val is not None else 0
        except RedisError:
            return 0

    async def info(self) -> dict[str, Any]:
        try:
            redis_info = await self._redis.info("memory")
            used_memory = redis_info.get("used_memory", 0)
        except RedisError:
            used_memory = 0
        return {
            "backend": "redis",
            "namespace": self._ns,
            "policy": self._policy.value,
            "entries": await self.size(),
            "cf_memory_bytes": await self.memory_bytes(),
            "redis_used_memory": used_memory,
            "max_entries": self._max_entries,
            "max_memory_bytes": self._max_memory_bytes,
        }

