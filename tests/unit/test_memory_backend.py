"""Unit tests for InMemoryBackend.

Covers:
 - get/set/delete/exists round-trips
 - TTL expiry with a fake clock
 - Manual invalidation (single key, prefix, flush)
 - Max-entry eviction for all three policies
 - Max-memory eviction
 - Version counters
"""
from __future__ import annotations

import time
from typing import Optional
from unittest.mock import patch

import pytest

from cacheforge.app.cache.base import CacheEntry
from cacheforge.app.cache.memory_backend import InMemoryBackend
from cacheforge.app.cache.policies.fifo import FIFOPolicy
from cacheforge.app.cache.policies.lfu import LFUPolicy
from cacheforge.app.cache.policies.lru import LRUPolicy
from cacheforge.app.cache.serializer import serialize


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _entry(
    key: str,
    value: str = '{"t":"s","v":"hello"}',
    ttl: Optional[int] = None,
    is_negative: bool = False,
    created_at: float = 0.0,
    last_accessed: float = 0.0,
    size_bytes: int = 64,
) -> CacheEntry:
    expires_at = (created_at + ttl) if ttl is not None else None
    return CacheEntry(
        key=key,
        value=value,
        ttl=ttl,
        created_at=created_at,
        expires_at=expires_at,
        last_accessed=last_accessed,
        size_bytes=size_bytes,
        is_negative=is_negative,
    )


@pytest.fixture
def lru_backend():
    return InMemoryBackend(policy=LRUPolicy(), max_entries=0, max_memory_bytes=0)


@pytest.fixture
def lru_limited():
    return InMemoryBackend(policy=LRUPolicy(), max_entries=3, max_memory_bytes=0)


@pytest.fixture
def mem_limited():
    # Allow up to 512 bytes; each entry is size_bytes=64 → max ~8 entries before overhead
    return InMemoryBackend(policy=LRUPolicy(), max_entries=0, max_memory_bytes=512)


# ── Basic CRUD ────────────────────────────────────────────────────────────────

class TestBasicCRUD:
    @pytest.mark.asyncio
    async def test_set_and_get(self, lru_backend):
        e = _entry("k1")
        await lru_backend.set(e)
        result = await lru_backend.get("k1")
        assert result is not None
        assert result.key == "k1"

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, lru_backend):
        assert await lru_backend.get("missing") is None

    @pytest.mark.asyncio
    async def test_exists_true(self, lru_backend):
        await lru_backend.set(_entry("k2"))
        assert await lru_backend.exists("k2") is True

    @pytest.mark.asyncio
    async def test_exists_false(self, lru_backend):
        assert await lru_backend.exists("no") is False

    @pytest.mark.asyncio
    async def test_delete_returns_true(self, lru_backend):
        await lru_backend.set(_entry("k3"))
        assert await lru_backend.delete("k3") is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_for_missing(self, lru_backend):
        assert await lru_backend.delete("ghost") is False

    @pytest.mark.asyncio
    async def test_overwrite(self, lru_backend):
        await lru_backend.set(_entry("k", value='{"t":"i","v":1}'))
        await lru_backend.set(_entry("k", value='{"t":"i","v":2}'))
        entry = await lru_backend.get("k")
        assert entry is not None
        assert entry.value == '{"t":"i","v":2}'


# ── TTL / Expiry ──────────────────────────────────────────────────────────────

class TestTTLExpiry:
    @pytest.mark.asyncio
    async def test_non_expired_entry_returned(self, lru_backend):
        """Entry with future expiry is returned normally."""
        now = time.time()
        e = _entry("live", ttl=300, created_at=now, last_accessed=now)
        e.expires_at = now + 300
        await lru_backend.set(e)
        result = await lru_backend.get("live")
        assert result is not None

    @pytest.mark.asyncio
    async def test_expired_entry_returns_none(self, lru_backend):
        """Entry whose expires_at is in the past returns None (lazy expiry)."""
        # Use a fake "now" that is far in the future so entry is expired
        past = time.time() - 1000
        e = _entry("stale", ttl=10, created_at=past - 10, last_accessed=past - 10)
        e.expires_at = past  # already past
        await lru_backend.set(e)

        result = await lru_backend.get("stale")
        assert result is None

    @pytest.mark.asyncio
    async def test_expired_entry_removed_from_store(self, lru_backend):
        """Expired lazy-eviction actually removes the key."""
        past = time.time() - 1000
        e = _entry("gone", ttl=1, created_at=past - 1, last_accessed=past - 1)
        e.expires_at = past
        await lru_backend.set(e)
        await lru_backend.get("gone")  # triggers lazy removal
        assert await lru_backend.size() == 0


# ── Prefix / Flush Invalidation ───────────────────────────────────────────────

class TestInvalidation:
    @pytest.mark.asyncio
    async def test_delete_by_prefix(self, lru_backend):
        await lru_backend.set(_entry("ns:a"))
        await lru_backend.set(_entry("ns:b"))
        await lru_backend.set(_entry("other:c"))
        removed = await lru_backend.delete_by_prefix("ns:")
        assert removed == 2
        assert await lru_backend.exists("ns:a") is False
        assert await lru_backend.exists("ns:b") is False
        assert await lru_backend.exists("other:c") is True

    @pytest.mark.asyncio
    async def test_flush(self, lru_backend):
        for i in range(5):
            await lru_backend.set(_entry(f"k{i}"))
        removed = await lru_backend.flush()
        assert removed == 5
        assert await lru_backend.size() == 0


# ── Eviction: max_entries ─────────────────────────────────────────────────────

class TestMaxEntriesEviction:
    @pytest.mark.asyncio
    async def test_lru_evicts_oldest_on_overflow(self):
        backend = InMemoryBackend(LRUPolicy(), max_entries=2)
        # A at t=1, B at t=2 — both in cache
        await backend.set(_entry("A", created_at=1.0, last_accessed=1.0))
        await backend.set(_entry("B", created_at=2.0, last_accessed=2.0))
        # Adding C evicts A (LRU)
        await backend.set(_entry("C", created_at=3.0, last_accessed=3.0))
        assert await backend.exists("A") is False
        assert await backend.exists("B") is True
        assert await backend.exists("C") is True

    @pytest.mark.asyncio
    async def test_lfu_evicts_least_frequent(self):
        backend = InMemoryBackend(LFUPolicy(), max_entries=2)
        await backend.set(_entry("A", last_accessed=1.0))
        await backend.set(_entry("B", last_accessed=2.0))
        # Access B many times
        for _ in range(5):
            e = await backend.get("B")
        # A has lower frequency → evicted when C arrives
        await backend.set(_entry("C", last_accessed=3.0))
        assert await backend.exists("A") is False
        assert await backend.exists("B") is True
        assert await backend.exists("C") is True

    @pytest.mark.asyncio
    async def test_fifo_evicts_first_inserted(self):
        backend = InMemoryBackend(FIFOPolicy(), max_entries=2)
        await backend.set(_entry("A", created_at=1.0))
        await backend.set(_entry("B", created_at=2.0))
        # Access A many times (FIFO ignores access)
        for _ in range(5):
            await backend.get("A")
        await backend.set(_entry("C", created_at=3.0))
        assert await backend.exists("A") is False  # inserted first
        assert await backend.exists("B") is True
        assert await backend.exists("C") is True

    @pytest.mark.asyncio
    async def test_size_never_exceeds_limit(self):
        backend = InMemoryBackend(LRUPolicy(), max_entries=3)
        for i in range(10):
            await backend.set(_entry(f"k{i}", last_accessed=float(i)))
        assert await backend.size() <= 3


# ── Eviction: max_memory_bytes ────────────────────────────────────────────────

class TestMaxMemoryEviction:
    @pytest.mark.asyncio
    async def test_memory_limit_enforced(self):
        # Each entry is size_bytes=200; limit=450 → max 2 entries
        backend = InMemoryBackend(LRUPolicy(), max_memory_bytes=450)
        for i in range(5):
            await backend.set(_entry(f"k{i}", size_bytes=200, last_accessed=float(i)))
        assert await backend.memory_bytes() <= 450

    @pytest.mark.asyncio
    async def test_memory_counter_decreases_on_eviction(self):
        backend = InMemoryBackend(LRUPolicy(), max_memory_bytes=300)
        await backend.set(_entry("a", size_bytes=200, last_accessed=1.0))
        await backend.set(_entry("b", size_bytes=200, last_accessed=2.0))
        # b should have triggered eviction of a; total = 200
        assert await backend.memory_bytes() <= 300


# ── Version counters ──────────────────────────────────────────────────────────

class TestVersions:
    @pytest.mark.asyncio
    async def test_initial_version_zero(self, lru_backend):
        assert await lru_backend.get_version("new_key") == 0

    @pytest.mark.asyncio
    async def test_increment_version(self, lru_backend):
        v1 = await lru_backend.increment_version("k")
        assert v1 == 1
        v2 = await lru_backend.increment_version("k")
        assert v2 == 2

    @pytest.mark.asyncio
    async def test_version_persists_after_delete(self, lru_backend):
        """Version counter must survive entry deletion (stale-repopulation guard)."""
        await lru_backend.set(_entry("k"))
        await lru_backend.increment_version("k")
        await lru_backend.delete("k")
        assert await lru_backend.get_version("k") == 1


class TestLifecycleAndSweeper:
    @pytest.mark.asyncio
    async def test_info_reporting(self, lru_backend):
        await lru_backend.set(_entry("infokey", size_bytes=100))
        info = await lru_backend.info()
        assert info["backend"] == "memory"
        assert info["entries"] == 1
        assert info["memory_bytes"] == 100

    @pytest.mark.asyncio
    async def test_background_sweeper(self):
        backend = InMemoryBackend(LRUPolicy(), sweep_interval=0.01)
        now = time.time()
        # Expired entry
        backend._store["exp"] = _entry("exp", created_at=now - 100, last_accessed=now - 100, ttl=5)
        backend._store["exp"].expires_at = now - 50

        # Run sweep directly
        removed = await backend._sweep_expired()
        assert removed == 1
        assert "exp" not in backend._store

        # Start and stop sweeper task
        backend.start_sweeper()
        assert backend._sweeper_task is not None
        await backend.stop_sweeper()
        assert backend._sweeper_task is None

