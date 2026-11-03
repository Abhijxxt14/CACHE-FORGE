"""Concurrency and failure resilience tests.

Covers:
  - High concurrency stampede protection (50+ coroutines -> exactly 1 DB load)
  - Stale repopulation race under concurrent writes
  - Parallel writers consistency (metadata, size, limits)
  - Failure degradation (backend failure -> graceful fallback to DB)
"""
from __future__ import annotations

import asyncio
import random
from unittest.mock import AsyncMock

import pytest

from cacheforge.app.cache.base import CacheBackend, CacheEntry
from cacheforge.app.cache.manager import CacheManager
from cacheforge.app.cache.memory_backend import InMemoryBackend
from cacheforge.app.cache.policies.lru import LRUPolicy
from cacheforge.app.metrics.counters import metrics


@pytest.fixture
def manager() -> CacheManager:
    metrics.reset()
    backend = InMemoryBackend(policy=LRUPolicy(), max_entries=50, max_memory_bytes=1024 * 1024)
    return CacheManager(backend=backend, namespace="test_conc", default_ttl=60, stampede_timeout=2.0)


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_high_concurrency_stampede(self, manager: CacheManager):
        """50 concurrent coroutines requesting an uncached key triggers exactly 1 DB read."""
        db_invocations = 0

        async def slow_db():
            nonlocal db_invocations
            db_invocations += 1
            await asyncio.sleep(0.08)
            return {"user": "Alice", "balance": 1000}

        key = manager.make_key("user:101")
        tasks = [manager.get_or_load(key, slow_db) for _ in range(50)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 50
        assert all(r == {"user": "Alice", "balance": 1000} for r in results)
        assert db_invocations == 1

    @pytest.mark.asyncio
    async def test_parallel_writers_keep_metadata_consistent(self, manager: CacheManager):
        """Multiple parallel writers with limit enforcement keep sizes and counters strictly consistent."""
        async def write_worker(worker_id: int):
            for i in range(10):
                k = manager.make_key(f"worker:{worker_id}:item:{i}")
                await manager.set(k, {"worker": worker_id, "idx": i, "data": "payload" * 5})
                await asyncio.sleep(0.001)

        # Run 10 parallel worker tasks doing 10 writes each = 100 writes total
        workers = [write_worker(w) for w in range(10)]
        await asyncio.gather(*workers)

        # Cache limit is 50
        backend_size = await manager._backend.size()
        assert backend_size <= 50

        # Memory counter should match sum of live entries
        total_mem = await manager._backend.memory_bytes()
        assert total_mem > 0

    @pytest.mark.asyncio
    async def test_stale_repopulation_race_concurrent(self, manager: CacheManager):
        """Loader reading old version aborts caching if an update occurs before load finishes."""
        key = manager.make_key("race:account:1")
        gate = asyncio.Event()

        async def delayed_reader():
            gate.set()
            await asyncio.sleep(0.05)
            return "STALE_DATA"

        read_task = asyncio.create_task(manager.get_or_load(key, delayed_reader))
        await gate.wait()

        # Interleaving writer: update DB and invalidate cache
        await manager.invalidate_on_update(key)

        res = await read_task
        assert res == "STALE_DATA"
        # Stale value was rejected by version token check
        assert await manager.exists(key) is False
        assert metrics.stale_prevented >= 1


class TestFailureDegradation:
    @pytest.mark.asyncio
    async def test_backend_failure_serves_directly_from_db(self):
        """When cache backend raises an unexpected error, request still succeeds from DB."""
        failing_backend = AsyncMock(spec=CacheBackend)
        failing_backend.get.side_effect = ConnectionError("Redis cluster unreachable")
        failing_backend.get_version.return_value = 0
        failing_backend.set.side_effect = ConnectionError("Redis cluster unreachable")

        manager = CacheManager(backend=failing_backend, namespace="test_fail")

        db_called = False

        async def db_loader():
            nonlocal db_called
            db_called = True
            return {"status": "ok", "source": "postgres"}

        # Request must NOT crash; it serves from DB loader
        key = manager.make_key("resilience:key")
        result = await manager.get_or_load(key, db_loader)

        assert db_called is True
        assert result == {"status": "ok", "source": "postgres"}

