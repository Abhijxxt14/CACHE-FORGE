"""Health-check router.

GET /health — returns status of the API, Redis, and PostgreSQL.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import asyncpg
from fastapi import APIRouter
from redis.asyncio import Redis

from cacheforge.app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


async def _check_redis() -> dict[str, Any]:
    start = time.perf_counter()
    try:
        r: Redis = Redis.from_url(settings.redis_url, socket_connect_timeout=2)
        await r.ping()
        await r.aclose()
        return {"status": "ok", "latency_ms": round((time.perf_counter() - start) * 1000, 2)}
    except Exception as exc:
        logger.warning("Redis health check failed: %s", exc)
        return {"status": "error", "detail": str(exc)}


async def _check_postgres() -> dict[str, Any]:
    start = time.perf_counter()
    try:
        conn = await asyncio.wait_for(
            asyncpg.connect(settings.database_url_plain),
            timeout=3.0,
        )
        await conn.execute("SELECT 1")
        await conn.close()
        return {"status": "ok", "latency_ms": round((time.perf_counter() - start) * 1000, 2)}
    except Exception as exc:
        logger.warning("Postgres health check failed: %s", exc)
        return {"status": "error", "detail": str(exc)}


@router.get("/health")
async def health() -> dict[str, Any]:
    """Return the health status of the API, Redis, and PostgreSQL."""
    redis_result, pg_result = await asyncio.gather(
        _check_redis(),
        _check_postgres(),
    )

    overall = (
        "ok"
        if redis_result["status"] == "ok" and pg_result["status"] == "ok"
        else "degraded"
    )

    return {
        "status": overall,
        "instance_id": settings.instance_id,
        "components": {
            "api": {"status": "ok"},
            "redis": redis_result,
            "postgres": pg_result,
        },
    }
