"""PostgreSQL database layer.

Uses SQLAlchemy 2.x async engine + asyncpg driver.

Tables:
  items — a simple key/value-ish table used to demonstrate cache-aside end-to-end.

Schema:
  id          BIGSERIAL PRIMARY KEY
  name        TEXT NOT NULL
  description TEXT
  value       JSONB
  created_at  TIMESTAMPTZ DEFAULT now()
  updated_at  TIMESTAMPTZ DEFAULT now()
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

from cacheforge.app.config import settings
from cacheforge.app.metrics.counters import metrics

logger = logging.getLogger(__name__)


# ── ORM base ──────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class Item(Base):
    """Demo item table used to exercise cache-aside end-to-end."""

    __tablename__ = "items"

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    value: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[Any] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.text("now()")
    )
    updated_at: Mapped[Any] = mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        onupdate=sa.text("now()"),
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "value": self.value,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


# ── Engine factory ────────────────────────────────────────────────────────────

_engine: Optional[AsyncEngine] = None


def get_engine() -> AsyncEngine:
    """Return the shared async SQLAlchemy engine, creating it on first call."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False,
        )
        # Query counter event listener
        @sa.event.listens_for(_engine.sync_engine, "after_execute")
        def _count_query(conn, clauseelement, multiparams, params, execution_options, result):  # noqa: ANN001
            metrics.record_db_query(0.0)  # latency tracked in CacheManager

    return _engine


async def init_db() -> None:
    """Create tables if they do not exist."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncSession:
    """Create and return a new async session (caller must close it)."""
    from sqlalchemy.ext.asyncio import async_sessionmaker
    factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return factory()


# ── Repository ────────────────────────────────────────────────────────────────

class ItemRepository:
    """Data-access object for the items table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, item_id: int) -> Optional[dict[str, Any]]:
        """Fetch a single item by ID."""
        t0 = time.perf_counter()
        result = await self._session.execute(
            sa.select(Item).where(Item.id == item_id)
        )
        row = result.scalars().first()
        metrics.record_db_query(time.perf_counter() - t0)
        return row.to_dict() if row else None

    async def list_items(
        self,
        limit: int = 20,
        offset: int = 0,
        name_filter: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """List items with optional name filter, pagination."""
        t0 = time.perf_counter()
        q = sa.select(Item).order_by(Item.id).limit(limit).offset(offset)
        if name_filter:
            q = q.where(Item.name.ilike(f"%{name_filter}%"))
        result = await self._session.execute(q)
        rows = result.scalars().all()
        metrics.record_db_query(time.perf_counter() - t0)
        return [r.to_dict() for r in rows]

    async def create(self, name: str, description: Optional[str] = None, value: Optional[dict] = None) -> dict[str, Any]:
        t0 = time.perf_counter()
        item = Item(name=name, description=description, value=value)
        self._session.add(item)
        await self._session.flush()
        await self._session.refresh(item)
        metrics.record_db_query(time.perf_counter() - t0)
        return item.to_dict()

    async def update(
        self,
        item_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None,
        value: Optional[dict] = None,
    ) -> Optional[dict[str, Any]]:
        t0 = time.perf_counter()
        result = await self._session.execute(
            sa.select(Item).where(Item.id == item_id)
        )
        item = result.scalars().first()
        if item is None:
            metrics.record_db_query(time.perf_counter() - t0)
            return None
        if name is not None:
            item.name = name
        if description is not None:
            item.description = description
        if value is not None:
            item.value = value
        await self._session.flush()
        await self._session.refresh(item)
        metrics.record_db_query(time.perf_counter() - t0)
        return item.to_dict()

    async def delete(self, item_id: int) -> bool:
        t0 = time.perf_counter()
        result = await self._session.execute(
            sa.delete(Item).where(Item.id == item_id).returning(Item.id)
        )
        metrics.record_db_query(time.perf_counter() - t0)
        return result.rowcount > 0

