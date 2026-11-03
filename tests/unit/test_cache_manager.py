"""Unit and concurrency tests for CacheManager.

Tests:
  - Cache-aside hit / miss flow
  - Loader invocation and cache population
  - Negative caching (not found cached with negative TTL)
  - Stampede protection (N concurrent misses -> exactly 1 loader invocation)
  - Stale-repopulation race protection (invalidation during DB read -> stale value not cached)
  - Invalidation on update
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from cacheforge.app.cache.base import CacheBackend
from cacheforge.app.cache.manager import CacheManager
from cacheforge.app.cache.memory_backend import InMemoryBackend
from cacheforge.app.cache.policies.lru import LRUPolicy
from cacheforge.app.metrics.counters import metrics


@pytest.fixture
def memory_backend() -> CacheBackend:
    return InMemoryBackend(policy=LRUPolicy(), max_entries=100, max_memory_bytes=1024 * 1024)


@pytest.fixture
def cache_manager(memory_backend: CacheBackend) -> CacheManager:
    metrics.reset()
    return CacheManager(
        backend=memory_backend,
        namespace="test",
        default_ttl=60,
        negative_ttl=10,
        ttl_jitter=0,
        stampede_timeout=2.0,
    )


class TestCacheAside:
    @pytest.mark.asyncio
    async def test_miss_invokes_loader_and_populates_cache(self, cache_manager: CacheManager):
        loader = AsyncMock(return_value={"id": 1, "name": "Apple"})
        key = cache_manager.make_key("item:1")

        # First read: cache miss -> loader called
        result = await cache_manager.get_or_load(key, loader)
        assert result == {"id": 1, "name": "Apple"}
        assert loader.await_count == 1
        assert metrics.cache_misses == 1
        assert metrics.cache_hits == 0

        # Second read: cache hit -> loader NOT called
        result2 = await cache_manager.get_or_load(key, loader)
        assert result2 == {"id": 1, "name": "Apple"}
        assert loader.await_count == 1
        assert metrics.cache_hits == 1

    @pytest.mark.asyncio
    async def test_negative_caching_for_missing_item(self, cache_manager: CacheManager):
        """When loader returns None, CacheManager caches negative entry."""
        loader = AsyncMock(return_value=None)
        key = cache_manager.make_key("item:missing")

        # First read: miss -> loader called -> caches None
        result = await cache_manager.get_or_load(key, loader)
        assert result is None
        assert loader.await_count == 1
        assert metrics.cache_misses == 1

        # Second read: hits negative cache -> loader NOT called
        result2 = await cache_manager.get_or_load(key, loader)
        assert result2 is None
        assert loader.await_count == 1
        assert metrics.negative_hits == 1

    @pytest.mark.asyncio
    async def test_invalidate_on_update(self, cache_manager: CacheManager):
        loader = AsyncMock(return_value="v1")
        key = cache_manager.make_key("item:2")

        await cache_manager.get_or_load(key, loader)
        assert loader.await_count == 1

        # Invalidate
        await cache_manager.invalidate_on_update(key)
        assert await cache_manager.exists(key) is False

        # Next read triggers loader again
        loader.return_value = "v2"
        res = await cache_manager.get_or_load(key, loader)
        assert res == "v2"
        assert loader.await_count == 2


class TestStampedeProtection:
    @pytest.mark.asyncio
    async def test_concurrent_misses_result_in_single_loader_call(self, cache_manager: CacheManager):
        """When 10 concurrent requests miss the same key, only 1 calls the loader."""
        call_count = 0

        async def slow_loader():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)  # simulate DB delay
            return {"data": 42}

        key = cache_manager.make_key("stampede:test")

        # Launch 10 concurrent requests
        results = await asyncio.gather(*(cache_manager.get_or_load(key, slow_loader) for _ in range(10)))

        # All 10 callers should receive the exact same data
        for r in results:
            assert r == {"data": 42}

        # The loader must have been invoked exactly ONCE!
        assert call_count == 1


class TestStaleRepopulationRace:
    @pytest.mark.asyncio
    async def test_invalidation_during_in_flight_load_prevents_stale_write(self, cache_manager: CacheManager):
        """If key is invalidated while DB load is in progress, stale data is returned to caller but NOT cached."""
        key = cache_manager.make_key("race:key")
        loader_started = asyncio.Event()

        async def slow_loader():
            loader_started.set()
            await asyncio.sleep(0.05)  # simulate DB load time
            return "stale_old_value"

        # Task 1: begins loading
        load_task = asyncio.create_task(cache_manager.get_or_load(key, slow_loader))

        # Wait until loader has started
        await loader_started.wait()

        # Invalidation occurs in parallel (e.g. an update happened!)
        await cache_manager.invalidate_on_update(key)

        # Loader finishes
        result = await load_task
        assert result == "stale_old_value"

        # Key must NOT be cached because version changed during the load
        assert await cache_manager.exists(key) is False
        assert metrics.stale_prevented >= 1

