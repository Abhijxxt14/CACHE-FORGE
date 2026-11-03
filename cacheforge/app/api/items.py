"""Demo items API endpoints backed by PostgreSQL via cache-aside.

Routes:
  GET    /items            — list items (paginated, with optional name filter)
  POST   /items            — create an item
  GET    /items/{id}       — get item by id
  PUT    /items/{id}       — update item (invalidates cache)
  DELETE /items/{id}       — delete item (invalidates cache)
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from cacheforge.app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/items", tags=["items"])


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class ItemCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    value: Optional[dict] = None


class ItemUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    value: Optional[dict] = None


# ── Dependency helpers ────────────────────────────────────────────────────────

# In-memory fallback store when PostgreSQL is unreachable
_FALLBACK_STORE: dict[int, dict[str, Any]] = {
    i: {
        "id": i,
        "name": f"Item-{i}",
        "description": f"Fallback item description for #{i}",
        "value": {"sku": f"SKU-{i:06d}", "price": round((i % 100) + 0.99, 2)},
        "created_at": "2026-11-03T00:00:00Z",
        "updated_at": "2026-11-03T00:00:00Z",
    }
    for i in range(1, 1001)
}


def _get_manager():
    from cacheforge.app.main import get_cache_manager
    return get_cache_manager()


_db_available: bool = True
_last_db_check: float = 0.0


async def _get_session():
    global _db_available, _last_db_check
    now = time.time()
    if not _db_available and (now - _last_db_check) < 10.0:
        return None

    _last_db_check = now
    from cacheforge.app.db.repository import get_session
    try:
        session = await get_session()
        _db_available = True
        return session
    except Exception:
        _db_available = False
        return None


def _item_cache_key(item_id: int) -> str:
    return f"items:{item_id}"


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("")
async def list_items(
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    name: Optional[str] = Query(default=None),
) -> list[dict[str, Any]]:
    """List items from PostgreSQL (no cache — list queries are not cached)."""
    from cacheforge.app.db.repository import ItemRepository
    session = await _get_session()
    if session is not None:
        try:
            async with session:
                repo = ItemRepository(session)
                return await repo.list_items(limit=limit, offset=offset, name_filter=name)
        except Exception as exc:
            logger.warning("DB list failed (%s); using fallback store", exc)

    items = list(_FALLBACK_STORE.values())
    if name:
        items = [it for it in items if name.lower() in it["name"].lower()]
    return items[offset : offset + limit]


@router.post("", status_code=201)
async def create_item(body: ItemCreate) -> dict[str, Any]:
    """Create a new item in PostgreSQL."""
    from cacheforge.app.db.repository import ItemRepository
    session = await _get_session()
    if session is not None:
        try:
            async with session.begin():
                repo = ItemRepository(session)
                return await repo.create(name=body.name, description=body.description, value=body.value)
        except Exception as exc:
            logger.warning("DB create failed (%s); using fallback store", exc)

    new_id = max(_FALLBACK_STORE.keys(), default=0) + 1
    item = {"id": new_id, "name": body.name, "description": body.description, "value": body.value}
    _FALLBACK_STORE[new_id] = item
    return item


@router.get("/{item_id}")
async def get_item(item_id: int) -> dict[str, Any]:
    """Get an item by ID, served from cache when possible."""
    from cacheforge.app.db.repository import ItemRepository
    from cacheforge.app.metrics.counters import metrics

    manager = _get_manager()
    cache_key = manager.make_key(_item_cache_key(item_id))

    async def _loader():
        global _db_available
        session = await _get_session()
        if session is not None:
            try:
                async with session:
                    repo = ItemRepository(session)
                    return await repo.get(item_id)
            except Exception as exc:
                _db_available = False
                logger.warning("DB read failed (%s); reading from fallback store", exc)

        # Fallback simulation with DB query counter
        t0 = time.perf_counter()
        import asyncio
        await asyncio.sleep(0.001)  # simulate DB I/O delay (1ms)
        item = _FALLBACK_STORE.get(item_id)
        metrics.record_db_query(time.perf_counter() - t0)
        return item

    if settings.cache_enabled:
        result = await manager.get_or_load(cache_key, _loader)
    else:
        result = await _loader()

    if result is None:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")
    return result


@router.put("/{item_id}")
async def update_item(item_id: int, body: ItemUpdate) -> dict[str, Any]:
    """Update an item and invalidate its cache entry."""
    from cacheforge.app.db.repository import ItemRepository
    from cacheforge.app.metrics.counters import metrics

    session = await _get_session()
    result = None
    if session is not None:
        try:
            async with session.begin():
                repo = ItemRepository(session)
                result = await repo.update(
                    item_id,
                    name=body.name,
                    description=body.description,
                    value=body.value,
                )
        except Exception as exc:
            logger.warning("DB update failed (%s); updating fallback store", exc)

    if result is None:
        if item_id not in _FALLBACK_STORE:
            raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")
        item = _FALLBACK_STORE[item_id]
        if body.name is not None:
            item["name"] = body.name
        if body.description is not None:
            item["description"] = body.description
        if body.value is not None:
            item["value"] = body.value
        result = item
        metrics.record_db_query(0.002)

    # Invalidate cache after successful DB update
    if settings.cache_enabled:
        manager = _get_manager()
        cache_key = manager.make_key(_item_cache_key(item_id))
        await manager.invalidate_on_update(cache_key)

    return result


@router.delete("/{item_id}", status_code=200)
async def delete_item(item_id: int) -> dict[str, Any]:
    """Delete an item and remove its cache entry."""
    from cacheforge.app.db.repository import ItemRepository
    from cacheforge.app.metrics.counters import metrics

    session = await _get_session()
    deleted = False
    if session is not None:
        try:
            async with session.begin():
                repo = ItemRepository(session)
                deleted = await repo.delete(item_id)
        except Exception as exc:
            logger.warning("DB delete failed (%s); deleting from fallback store", exc)

    if not deleted:
        if item_id in _FALLBACK_STORE:
            del _FALLBACK_STORE[item_id]
            deleted = True
            metrics.record_db_query(0.002)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found.")

    # Invalidate cache
    if settings.cache_enabled:
        manager = _get_manager()
        cache_key = manager.make_key(_item_cache_key(item_id))
        await manager.invalidate_on_update(cache_key)

    return {"id": item_id, "deleted": True}

