"""Database seed script for CacheForge demo items.

Usage:
    python -m cacheforge.app.db.seed [--count 10000] [--batch-size 1000]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time

from sqlalchemy.dialects.postgresql import insert

from cacheforge.app.config import settings
from cacheforge.app.db.repository import Item, get_engine, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def seed_items(count: int = 10_000, batch_size: int = 1000) -> None:
    """Seed `count` items into the items table in batches."""
    logger.info("Initializing database tables...")
    await init_db()

    engine = get_engine()
    start_time = time.perf_counter()
    logger.info("Starting seed: inserting %d items (batch_size=%d)...", count, batch_size)

    inserted = 0
    for batch_start in range(1, count + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, count)
        items_data = [
            {
                "id": i,
                "name": f"Item-{i}",
                "description": f"Generated description for item #{i}",
                "value": {"sku": f"SKU-{i:06d}", "price": round((i % 100) + 0.99, 2), "stock": (i * 7) % 500},
            }
            for i in range(batch_start, batch_end + 1)
        ]

        async with engine.begin() as conn:
            stmt = insert(Item).values(items_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Item.id],
                set_={
                    "name": stmt.excluded.name,
                    "description": stmt.excluded.description,
                    "value": stmt.excluded.value,
                },
            )
            await conn.execute(stmt)

        inserted += len(items_data)
        if inserted % (batch_size * 5) == 0 or inserted == count:
            logger.info("Inserted %d / %d items (%.1f%%)...", inserted, count, (inserted / count) * 100)

    elapsed = time.perf_counter() - start_time
    logger.info("Seeding completed: %d items in %.2f seconds (%.0f items/sec)", inserted, elapsed, inserted / elapsed if elapsed > 0 else 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed database with demo items")
    parser.add_argument("--count", type=int, default=10_000, help="Number of items to seed (default: 10,000)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for insertions (default: 1,000)")
    args = parser.parse_args()

    asyncio.run(seed_items(count=args.count, batch_size=args.batch_size))


if __name__ == "__main__":
    main()

