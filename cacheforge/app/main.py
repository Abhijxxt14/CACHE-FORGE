"""CacheForge FastAPI application factory with lifespan and metrics middleware."""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, Response
from redis.asyncio import Redis

from cacheforge.app.api.cache import router as cache_router
from cacheforge.app.api.health import router as health_router
from cacheforge.app.api.items import router as items_router
from cacheforge.app.api.metrics import router as metrics_router
from cacheforge.app.cache.base import CacheBackend
from cacheforge.app.cache.manager import CacheManager
from cacheforge.app.cache.memory_backend import InMemoryBackend
from cacheforge.app.cache.policies.fifo import FIFOPolicy
from cacheforge.app.cache.policies.lfu import LFUPolicy
from cacheforge.app.cache.policies.lru import LRUPolicy
from cacheforge.app.cache.redis_backend import RedisBackend
from cacheforge.app.config import EvictionPolicyEnum, settings
from cacheforge.app.db.repository import init_db
from cacheforge.app.metrics.counters import metrics

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Global cache manager instance
_cache_manager: Optional[CacheManager] = None
_redis_client: Optional[Redis] = None


def get_cache_manager() -> CacheManager:
    """Return the global CacheManager instance."""
    global _cache_manager
    if _cache_manager is None:
        # Fallback to in-memory if uninitialized
        policy_cls = {
            EvictionPolicyEnum.lru: LRUPolicy,
            EvictionPolicyEnum.lfu: LFUPolicy,
            EvictionPolicyEnum.fifo: FIFOPolicy,
        }.get(settings.cache_eviction_policy, LRUPolicy)
        backend = InMemoryBackend(
            policy=policy_cls(),
            max_entries=settings.cache_max_entries,
            max_memory_bytes=settings.cache_max_memory_bytes,
        )
        _cache_manager = CacheManager(backend=backend)
    return _cache_manager


def set_cache_manager(manager: CacheManager) -> None:
    """Override the global CacheManager (primarily for testing)."""
    global _cache_manager
    _cache_manager = manager


def build_cache_backend(redis_client: Optional[Redis] = None) -> CacheBackend:
    """Instantiate the configured cache backend."""
    if redis_client is not None:
        return RedisBackend(
            redis=redis_client,
            namespace=settings.cache_namespace,
            policy=settings.cache_eviction_policy,
            max_entries=settings.cache_max_entries,
            max_memory_bytes=settings.cache_max_memory_bytes,
        )

    # In-memory backend
    policy_cls = {
        EvictionPolicyEnum.lru: LRUPolicy,
        EvictionPolicyEnum.lfu: LFUPolicy,
        EvictionPolicyEnum.fifo: FIFOPolicy,
    }.get(settings.cache_eviction_policy, LRUPolicy)

    backend = InMemoryBackend(
        policy=policy_cls(),
        max_entries=settings.cache_max_entries,
        max_memory_bytes=settings.cache_max_memory_bytes,
    )
    backend.start_sweeper()
    return backend


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    global _cache_manager, _redis_client
    logger.info("CacheForge starting up (instance=%s)", settings.instance_id)

    # 1. Initialize PostgreSQL tables (non-fatal if DB not yet available)
    try:
        await init_db()
        logger.info("Database initialized successfully.")
    except Exception as exc:
        logger.warning("Database initialization skipped or failed: %s", exc)

    # 2. Connect to Redis
    try:
        _redis_client = Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=3,
            decode_responses=False,
        )
        await _redis_client.ping()
        backend = build_cache_backend(_redis_client)
        _cache_manager = CacheManager(backend=backend)
        logger.info("Connected to Redis at %s", settings.redis_url)
    except Exception as exc:
        logger.warning("Redis connection failed (%s); degrading to in-memory cache", exc)
        metrics.record_redis_unavailable()
        backend = build_cache_backend(None)
        _cache_manager = CacheManager(backend=backend)

    yield

    # Shutdown
    logger.info("CacheForge shutting down (instance=%s)", settings.instance_id)
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception:
            pass


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    application = FastAPI(
        title="CacheForge",
        description="Distributed caching system with pluggable eviction policies.",
        version="0.1.0",
        lifespan=lifespan,
    )

    # ── Latency & Metrics Middleware ──────────────────────────────────────
    @application.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        start_time = time.perf_counter()
        response: Response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        metrics.record_request(elapsed_ms)
        response.headers["X-Process-Time-Ms"] = f"{elapsed_ms:.2f}"
        return response

    # ── Routers ────────────────────────────────────────────────────────────
    application.include_router(health_router)
    application.include_router(cache_router)
    application.include_router(items_router)
    application.include_router(metrics_router)

    return application


# Module-level app object for uvicorn
app = create_app()
