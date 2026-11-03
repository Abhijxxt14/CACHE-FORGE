"""Cache management API endpoints.

Routes:
  PUT    /cache/{key}        — set a value with optional TTL
  GET    /cache/{key}        — get a value
  DELETE /cache/{key}        — delete a value
  POST   /cache/invalidate   — invalidate by key or prefix
  GET    /cache/stats        — cache statistics
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from cacheforge.app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/cache", tags=["cache"])


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class SetRequest(BaseModel):
    value: Any = Field(..., description="Value to cache (JSON: dict, list, str, int, float, bool, null).")
    ttl: Optional[int] = Field(None, ge=1, description="Per-key TTL in seconds.")


class InvalidateRequest(BaseModel):
    key: Optional[str] = Field(None, description="Exact namespaced key to delete.")
    prefix: Optional[str] = Field(None, description="Namespace prefix to delete (all matching keys).")
    flush: bool = Field(False, description="If true, flush all entries.")


# ── Dependency: CacheManager ──────────────────────────────────────────────────

def _get_manager():
    """Return the app-level CacheManager from the request state."""
    from cacheforge.app.main import get_cache_manager
    return get_cache_manager()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.put("/{key:path}")
async def set_value(key: str, body: SetRequest) -> dict[str, Any]:
    """Store a value in the cache under *key* with an optional TTL."""
    if not settings.cache_enabled:
        return {"cached": False, "reason": "cache_disabled"}
    manager = _get_manager()
    ns_key = manager.make_key(key)
    await manager.set(ns_key, body.value, ttl=body.ttl)
    return {"key": ns_key, "cached": True, "ttl": body.ttl or settings.cache_default_ttl}


@router.get("/stats")
async def cache_stats() -> dict[str, Any]:
    """Return cache statistics (hits, misses, memory, evictions, etc.)."""
    manager = _get_manager()
    return await manager.stats()


@router.get("/{key:path}")
async def get_value(key: str) -> dict[str, Any]:
    """Retrieve a cached value by *key*."""
    if not settings.cache_enabled:
        raise HTTPException(status_code=503, detail="Cache is disabled.")
    manager = _get_manager()
    ns_key = manager.make_key(key)
    value = await manager.get(ns_key)
    if value is None and not await manager.exists(ns_key):
        raise HTTPException(status_code=404, detail=f"Key {ns_key!r} not found in cache.")
    return {"key": ns_key, "value": value}


@router.delete("/{key:path}")
async def delete_value(key: str) -> dict[str, Any]:
    """Delete a cached entry."""
    manager = _get_manager()
    ns_key = manager.make_key(key)
    deleted = await manager.delete(ns_key)
    return {"key": ns_key, "deleted": deleted}


@router.post("/invalidate")
async def invalidate(body: InvalidateRequest) -> dict[str, Any]:
    """Invalidate cache entries by key, prefix, or flush everything."""
    manager = _get_manager()

    if body.flush:
        count = await manager.flush()
        return {"flushed": True, "deleted": count}

    if body.key:
        deleted = await manager.delete(body.key)
        return {"key": body.key, "deleted": deleted}

    if body.prefix:
        count = await manager.invalidate_prefix(body.prefix)
        return {"prefix": body.prefix, "deleted": count}

    raise HTTPException(status_code=400, detail="Provide 'key', 'prefix', or set 'flush=true'.")

