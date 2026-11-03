"""In-process metrics counters and latency histograms.

All counters are module-level singletons so they accumulate across requests.
Thread/task safety is not a concern since asyncio is single-threaded per process;
the counters use plain Python integers which are updated atomically in CPython.

Metrics collected:
  - hits / misses / negative hits / stampede fallbacks / stale prevented
  - DB query count and cumulative latency
  - Request count and per-request latency (P50/P95/P99 via a reservoir)
  - Redis unavailability events
"""
from __future__ import annotations

import math
import time
from collections import deque
from typing import Any


class LatencyReservoir:
    """A fixed-size reservoir for computing approximate latency percentiles.

    Uses a simple deque that discards the oldest observations when full.
    For production use a t-digest or HDR histogram; this is sufficient for
    the purposes of this project.
    """

    def __init__(self, max_size: int = 10_000) -> None:
        self._data: deque[float] = deque(maxlen=max_size)

    def record(self, value_ms: float) -> None:
        self._data.append(value_ms)

    def percentile(self, p: float) -> float:
        """Return the *p*-th percentile (0–100) of recorded values."""
        if not self._data:
            return 0.0
        sorted_data = sorted(self._data)
        idx = math.ceil(p / 100 * len(sorted_data)) - 1
        return sorted_data[max(0, idx)]

    def snapshot(self) -> dict[str, float]:
        if not self._data:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
        return {
            "p50": self.percentile(50),
            "p95": self.percentile(95),
            "p99": self.percentile(99),
            "avg": sum(self._data) / len(self._data),
            "count": len(self._data),
        }


class Metrics:
    """Singleton metrics store for one CacheForge process instance."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """Reset all counters to zero (useful in tests)."""
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self.negative_hits: int = 0
        self.stampede_fallbacks: int = 0
        self.stale_prevented: int = 0
        self.db_query_count: int = 0
        self.db_query_latency_s: float = 0.0
        self.redis_unavailable: int = 0
        self.request_count: int = 0
        self._request_latency = LatencyReservoir()
        self._db_latency = LatencyReservoir()
        self._start_time: float = time.time()

    # ── Record methods ────────────────────────────────────────────────────

    def record_hit(self, key: str = "") -> None:
        self.cache_hits += 1

    def record_miss(self, key: str = "") -> None:
        self.cache_misses += 1

    def record_negative_hit(self, key: str = "") -> None:
        self.negative_hits += 1

    def record_stampede_fallback(self, key: str = "") -> None:
        self.stampede_fallbacks += 1

    def record_stale_prevented(self, key: str = "") -> None:
        self.stale_prevented += 1

    def record_db_query(self, latency_s: float) -> None:
        self.db_query_count += 1
        self.db_query_latency_s += latency_s
        self._db_latency.record(latency_s * 1000)

    def record_redis_unavailable(self) -> None:
        self.redis_unavailable += 1

    def record_request(self, latency_ms: float) -> None:
        self.request_count += 1
        self._request_latency.record(latency_ms)

    # ── Computed properties ───────────────────────────────────────────────

    @property
    def hit_ratio(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0

    @property
    def miss_ratio(self) -> float:
        return 1.0 - self.hit_ratio

    @property
    def avg_db_latency_ms(self) -> float:
        return (self.db_query_latency_s * 1000 / self.db_query_count) if self.db_query_count else 0.0

    # ── Snapshot ──────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of all metrics."""
        uptime = time.time() - self._start_time
        rps = self.request_count / uptime if uptime > 0 else 0.0
        return {
            "uptime_s": round(uptime, 2),
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "negative_hits": self.negative_hits,
                "hit_ratio": round(self.hit_ratio, 4),
                "miss_ratio": round(self.miss_ratio, 4),
                "stampede_fallbacks": self.stampede_fallbacks,
                "stale_prevented": self.stale_prevented,
            },
            "db": {
                "query_count": self.db_query_count,
                "avg_latency_ms": round(self.avg_db_latency_ms, 3),
                "latency": self._db_latency.snapshot(),
                "redis_unavailable_events": self.redis_unavailable,
            },
            "requests": {
                "count": self.request_count,
                "rps": round(rps, 2),
                "latency_ms": self._request_latency.snapshot(),
            },
        }


# Module-level singleton — import this from everywhere
metrics = Metrics()

