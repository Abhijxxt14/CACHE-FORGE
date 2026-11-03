"""Unit tests for FastAPI endpoints: cache ops, demo items, and metrics."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from cacheforge.app.cache.manager import CacheManager
from cacheforge.app.cache.memory_backend import InMemoryBackend
from cacheforge.app.cache.policies.lru import LRUPolicy
from cacheforge.app.main import app, set_cache_manager
from cacheforge.app.metrics.counters import metrics


@pytest.fixture(autouse=True)
def setup_test_cache():
    metrics.reset()
    backend = InMemoryBackend(policy=LRUPolicy(), max_entries=100)
    manager = CacheManager(backend=backend, namespace="test", default_ttl=60)
    set_cache_manager(manager)
    yield manager


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


class TestCacheEndpoints:
    @pytest.mark.asyncio
    async def test_put_and_get_cache(self, client: AsyncClient):
        # PUT
        put_res = await client.put("/cache/foo", json={"value": {"hello": "world"}, "ttl": 100})
        assert put_res.status_code == 200
        assert put_res.json()["cached"] is True

        # GET
        get_res = await client.get("/cache/foo")
        assert get_res.status_code == 200
        assert get_res.json()["value"] == {"hello": "world"}

    @pytest.mark.asyncio
    async def test_get_nonexistent_key_returns_404(self, client: AsyncClient):
        res = await client.get("/cache/doesnotexist")
        assert res.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_cache_key(self, client: AsyncClient):
        await client.put("/cache/to_delete", json={"value": 123})
        del_res = await client.delete("/cache/to_delete")
        assert del_res.status_code == 200
        assert del_res.json()["deleted"] is True

        get_res = await client.get("/cache/to_delete")
        assert get_res.status_code == 404

    @pytest.mark.asyncio
    async def test_invalidate_by_prefix_and_flush(self, client: AsyncClient):
        await client.put("/cache/pref:1", json={"value": "a"})
        await client.put("/cache/pref:2", json={"value": "b"})
        await client.put("/cache/other:3", json={"value": "c"})

        # Invalidate prefix "test:pref:"
        inv_res = await client.post("/cache/invalidate", json={"prefix": "test:pref:"})
        assert inv_res.status_code == 200
        assert inv_res.json()["deleted"] == 2

        # Flush
        flush_res = await client.post("/cache/invalidate", json={"flush": True})
        assert flush_res.status_code == 200
        assert flush_res.json()["flushed"] is True

    @pytest.mark.asyncio
    async def test_cache_stats(self, client: AsyncClient):
        res = await client.get("/cache/stats")
        assert res.status_code == 200
        data = res.json()
        assert "entries" in data
        assert "cache" in data


class TestMetricsEndpoint:
    @pytest.mark.asyncio
    async def test_metrics_endpoint_returns_counters(self, client: AsyncClient):
        res = await client.get("/metrics")
        assert res.status_code == 200
        data = res.json()
        assert "cache" in data
        assert "hits" in data["cache"]
        assert "misses" in data["cache"]
        assert "db" in data
        assert "requests" in data


class TestItemsEndpoints:
    @pytest.mark.asyncio
    async def test_get_item_cache_aside_flow(self, client: AsyncClient):
        item_data = {"id": 42, "name": "Widget", "description": "Useful", "value": {"stock": 10}}
        with patch("cacheforge.app.db.repository.ItemRepository.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = item_data

            # 1st request: Miss -> calls DB
            res1 = await client.get("/items/42")
            assert res1.status_code == 200
            assert res1.json() == item_data
            assert mock_get.await_count == 1

            # 2nd request: Hit -> cached, DB not called again
            res2 = await client.get("/items/42")
            assert res2.status_code == 200
            assert res2.json() == item_data
            assert mock_get.await_count == 1

    @pytest.mark.asyncio
    async def test_update_item_invalidates_cache(self, client: AsyncClient):
        item_data = {"id": 10, "name": "Initial"}
        updated_data = {"id": 10, "name": "Updated"}

        with (
            patch("cacheforge.app.db.repository.ItemRepository.get", new_callable=AsyncMock) as mock_get,
            patch("cacheforge.app.db.repository.ItemRepository.update", new_callable=AsyncMock) as mock_update,
        ):
            mock_get.return_value = item_data
            mock_update.return_value = updated_data

            # Populate cache
            res1 = await client.get("/items/10")
            assert res1.status_code == 200
            assert mock_get.await_count == 1

            # Update item -> should invalidate cache
            put_res = await client.put("/items/10", json={"name": "Updated"})
            assert put_res.status_code == 200

            # Next GET should read from DB again
            mock_get.return_value = updated_data
            res2 = await client.get("/items/10")
            assert res2.status_code == 200
            assert res2.json()["name"] == "Updated"
            assert mock_get.await_count == 2

