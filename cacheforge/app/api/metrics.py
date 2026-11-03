"""Metrics API endpoint.

GET /metrics — returns all collected performance metrics as JSON.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from cacheforge.app.metrics.counters import metrics

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def get_metrics() -> dict[str, Any]:
    """Return all CacheForge performance metrics as JSON."""
    return metrics.snapshot()

