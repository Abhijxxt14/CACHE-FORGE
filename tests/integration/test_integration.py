"""Integration tests: multi-instance consistency, cache-aside, and invalidation."""
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
def shared_backend() -> CacheBackend:
    """A shared backend mimicking shared Redis storage across multiple API instances."""
    return InMemoryBackend(policy=LRUPolicy(), max_entries=1000, max_memory_bytes=10 * 1024 * 1024)


@pytest.fixture
def app1_manager(shared_backend: CacheBackend) -> CacheManager:
    return CacheManager(backend=shared_backend, namespace="cf_cluster", default_ttl=300)


@pytest.fixture
def app2_manager(shared_backend: CacheBackend) -> CacheManager:
    return CacheManager(backend=shared_backend, namespace="cf_cluster", default_ttl=300)


class TestMultiInstanceConsistency:
    @pytest.mark.asyncio
    async def test_writes_on_instance1_are_immediately_visible_on_instance2(
        self, app1_manager: CacheManager, app2_manager: CacheManager
    ):
        """Cross-instance consistency: Instance 1 sets a key, Instance 2 reads it immediately."""
        key = app1_manager.make_key("shared:item:1")
        await app1_manager.set(key, {"name": "Widget X", "price": 49.99})

        # Instance 2 reads without accessing DB
        db_loader = AsyncMock()
        val = await app2_manager.get_or_load(key, db_loader)
        assert val == {"name": "Widget X", "price": 49.99}
        db_loader.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalidation_on_instance1_is_immediately_visible_on_instance2(
        self, app1_manager: CacheManager, app2_manager: CacheManager
    ):
        """Cross-instance invalidation: Instance 1 invalidates, Instance 2 must reload from DB."""
        key = app1_manager.make_key("shared:item:2")
        await app1_manager.set(key, "old_version")

        # Instance 1 updates DB and invalidates cache
        await app1_manager.invalidate_on_update(key)

        # Instance 2 tries to read -> must encounter cache miss and call DB
        db_loader = AsyncMock(return_value="new_version")
        val = await app2_manager.get_or_load(key, db_loader)
        assert val == "new_version"
        assert db_loader.await_count == 1

    @pytest.mark.asyncio
    async def test_prefix_invalidation_cross_instance(
        self, app1_manager: CacheManager, app2_manager: CacheManager
    ):
        """Instance 1 invalidates a namespace prefix; Instance 2 sees all matching keys removed."""
        k1 = app1_manager.make_key("catalog:electronics:tv")
        k2 = app1_manager.make_key("catalog:electronics:radio")
        k3 = app1_manager.make_key("catalog:books:novel")

        await app1_manager.set(k1, "tv")
        await app1_manager.set(k2, "radio")
        await app1_manager.set(k3, "novel")

        # Instance 1 deletes prefix
        deleted = await app1_manager.invalidate_prefix("cf_cluster:catalog:electronics:")
        assert deleted == 2

        # Instance 2 checks
        assert await app2_manager.exists(k1) is False
        assert await app2_manager.exists(k2) is False
        assert await app2_manager.exists(k3) is True

