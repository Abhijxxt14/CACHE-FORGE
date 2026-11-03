"""Phase 1 — health endpoint unit tests.

Tests the health() handler and its helper functions directly (without spinning
up an ASGI server or real network connections) using AsyncMock patches.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch


class TestHealthHandler:
    """Test the health() route handler directly."""

    @pytest.mark.asyncio
    async def test_health_ok_when_all_deps_healthy(self):
        from cacheforge.app.api.health import health
        with (
            patch("cacheforge.app.api.health._check_redis", new_callable=AsyncMock) as mr,
            patch("cacheforge.app.api.health._check_postgres", new_callable=AsyncMock) as mp,
        ):
            mr.return_value = {"status": "ok", "latency_ms": 1.0}
            mp.return_value = {"status": "ok", "latency_ms": 2.0}
            result = await health()

        assert result["status"] == "ok"
        assert result["components"]["api"]["status"] == "ok"
        assert result["components"]["redis"]["status"] == "ok"
        assert result["components"]["postgres"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_health_degraded_when_redis_down(self):
        from cacheforge.app.api.health import health
        with (
            patch("cacheforge.app.api.health._check_redis", new_callable=AsyncMock) as mr,
            patch("cacheforge.app.api.health._check_postgres", new_callable=AsyncMock) as mp,
        ):
            mr.return_value = {"status": "error", "detail": "Connection refused"}
            mp.return_value = {"status": "ok", "latency_ms": 2.0}
            result = await health()

        assert result["status"] == "degraded"
        assert result["components"]["redis"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_health_degraded_when_postgres_down(self):
        from cacheforge.app.api.health import health
        with (
            patch("cacheforge.app.api.health._check_redis", new_callable=AsyncMock) as mr,
            patch("cacheforge.app.api.health._check_postgres", new_callable=AsyncMock) as mp,
        ):
            mr.return_value = {"status": "ok", "latency_ms": 1.0}
            mp.return_value = {"status": "error", "detail": "could not connect"}
            result = await health()

        assert result["status"] == "degraded"
        assert result["components"]["postgres"]["status"] == "error"

    @pytest.mark.asyncio
    async def test_health_includes_instance_id(self):
        from cacheforge.app.api.health import health
        with (
            patch("cacheforge.app.api.health._check_redis", new_callable=AsyncMock) as mr,
            patch("cacheforge.app.api.health._check_postgres", new_callable=AsyncMock) as mp,
        ):
            mr.return_value = {"status": "ok", "latency_ms": 1.0}
            mp.return_value = {"status": "ok", "latency_ms": 1.0}
            result = await health()

        assert "instance_id" in result
