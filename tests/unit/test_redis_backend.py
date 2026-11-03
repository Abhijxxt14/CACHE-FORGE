"""Unit tests for RedisBackend using an async mock client.

Verifies:
  - get / set / delete / exists operations
  - Lua script invocations and parameter handling
  - Version checking and atomic updates
  - Distributed lock acquisition and release
  - Eviction threshold calculation
  - SCAN-based prefix deletion and flush
  - Error handling and metrics reporting on RedisError
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import RedisError

from cacheforge.app.cache.base import CacheEntry
from cacheforge.app.cache.redis_backend import RedisBackend
from cacheforge.app.config import EvictionPolicyEnum
from cacheforge.app.metrics.counters import metrics


@pytest.fixture
def mock_redis() -> MagicMock:
    r = MagicMock()
    r.register_script = MagicMock(return_value=AsyncMock())
    r.get = AsyncMock()
    r.set = AsyncMock()
    r.delete = AsyncMock()
    r.exists = AsyncMock()
    r.zcard = AsyncMock()
    r.hget = AsyncMock()
    r.hincrby = AsyncMock()
    r.info = AsyncMock()
    r.scan_iter = MagicMock()
    return r


@pytest.fixture
def redis_backend(mock_redis: MagicMock) -> RedisBackend:
    metrics.reset()
    return RedisBackend(
        redis=mock_redis,
        namespace="cf_test",
        policy=EvictionPolicyEnum.lru,
        max_entries=100,
        max_memory_bytes=1024 * 1024,
    )


class TestRedisBackendOps:
    @pytest.mark.asyncio
    async def test_get_existing_entry(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.get.return_value = b'{"t":"s","v":"hello world"}'
        mock_script = mock_redis.register_script.return_value
        mock_script.return_value = 1

        entry = await redis_backend.get("cf_test:key1")
        assert entry is not None
        assert entry.key == "cf_test:key1"
        mock_redis.get.assert_awaited_once_with("cf_test:key1")
        mock_script.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_missing_entry_returns_none(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.get.return_value = None
        entry = await redis_backend.get("cf_test:nonexistent")
        assert entry is None

    @pytest.mark.asyncio
    async def test_set_entry_invokes_lua_script(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.zcard.return_value = 10
        mock_redis.hget.return_value = "5000"
        mock_script = mock_redis.register_script.return_value
        mock_script.return_value = 1

        entry = CacheEntry(
            key="cf_test:key2",
            value='{"t":"i","v":999}',
            ttl=120,
            version=0,
            size_bytes=128,
        )

        await redis_backend.set(entry)
        mock_script.assert_awaited_once()
        args = mock_script.call_args[1]["args"]
        assert args[0] == '{"t":"i","v":999}'
        assert args[1] == 120000  # ttl in ms
        assert args[3] == 128     # size_bytes

    @pytest.mark.asyncio
    async def test_delete_entry(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_script = mock_redis.register_script.return_value
        mock_script.return_value = 2  # new version

        res = await redis_backend.delete("cf_test:key3")
        assert res is True
        mock_script.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_exists(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.exists.return_value = 1
        assert await redis_backend.exists("cf_test:key") is True

        mock_redis.exists.return_value = 0
        assert await redis_backend.exists("cf_test:key") is False

    @pytest.mark.asyncio
    async def test_delete_by_prefix_uses_scan(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        async def fake_scan(match, count):
            yield b"cf_test:prefix:1"
            yield b"cf_test:prefix:2"

        mock_redis.scan_iter = fake_scan
        mock_script = mock_redis.register_script.return_value
        mock_script.return_value = 1

        deleted = await redis_backend.delete_by_prefix("cf_test:prefix:")
        assert deleted == 2

    @pytest.mark.asyncio
    async def test_flush_uses_scan(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        async def fake_scan(match, count):
            yield b"cf_test:k1"
            yield b"cf_test:k2"
            yield b"cf_test:k3"

        mock_redis.scan_iter = fake_scan
        mock_redis.delete = AsyncMock(return_value=1)

        count = await redis_backend.flush()
        assert count == 3
        assert mock_redis.delete.await_count == 3


class TestRedisLockingAndVersioning:
    @pytest.mark.asyncio
    async def test_acquire_and_release_lock(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.set.return_value = True
        mock_script = mock_redis.register_script.return_value
        mock_script.return_value = 1

        # Acquire lock
        acquired = await redis_backend.acquire_lock("cf_test:data:1", "token-abc", ttl_ms=5000)
        assert acquired is True
        mock_redis.set.assert_awaited_once_with("cf_test:__lock:cf_test:data:1", "token-abc", nx=True, px=5000)

        # Release lock
        released = await redis_backend.release_lock("cf_test:data:1", "token-abc")
        assert released is True

    @pytest.mark.asyncio
    async def test_version_increment_and_get(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.hincrby.return_value = 4
        mock_redis.hget.return_value = b"4"

        v = await redis_backend.increment_version("cf_test:data:1")
        assert v == 4

        cur = await redis_backend.get_version("cf_test:data:1")
        assert cur == 4


class TestRedisErrorDegradation:
    @pytest.mark.asyncio
    async def test_redis_errors_trigger_metrics_and_do_not_crash(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.get.side_effect = RedisError("Network timeout")
        mock_redis.exists.side_effect = RedisError("Timeout")
        mock_redis.delete.side_effect = RedisError("Timeout")
        mock_redis.hget.side_effect = RedisError("Timeout")
        mock_redis.hincrby.side_effect = RedisError("Timeout")

        # GET
        assert await redis_backend.get("cf_test:err") is None
        assert metrics.redis_unavailable >= 1

        # EXISTS
        assert await redis_backend.exists("cf_test:err") is False

        # Versioning
        assert await redis_backend.get_version("cf_test:err") == 0
        assert await redis_backend.increment_version("cf_test:err") == 0


class TestRedisEvictionLogic:
    @pytest.mark.asyncio
    async def test_evict_if_needed_when_exceeding_limits(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        # 1st loop: zcard=150 (exceeds max_entries=100) -> evict
        # 2nd loop: zcard=80 (under limit) -> break
        mock_redis.zcard.side_effect = [150, 80]
        mock_redis.hget.return_value = "100"

        evict_script = AsyncMock(return_value=[b"victim1", b"victim2"])
        # Mock evict_script
        redis_backend._evict_script = evict_script

        await redis_backend._evict_if_needed(incoming_bytes=100)
        assert evict_script.await_count == 1

    @pytest.mark.asyncio
    async def test_info_and_diagnostics(self, redis_backend: RedisBackend, mock_redis: MagicMock):
        mock_redis.zcard.return_value = 42
        mock_redis.hget.return_value = "12345"
        mock_redis.info.return_value = {"used_memory": 67890}

        info = await redis_backend.info()
        assert info["backend"] == "redis"
        assert info["entries"] == 42
        assert info["cf_memory_bytes"] == 12345
        assert info["redis_used_memory"] == 67890

    @pytest.mark.asyncio
    async def test_lfu_backend_score_and_keys(self, mock_redis: MagicMock):
        lfu_b = RedisBackend(redis=mock_redis, namespace="lfu_ns", policy=EvictionPolicyEnum.lfu)
        assert lfu_b._score() == 0.0
        assert "freq" in lfu_b._freq_key()
