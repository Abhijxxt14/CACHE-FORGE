"""Unit tests for all three eviction policies.

Tests use a synthetic ``CacheEntry`` with a controllable clock so no real
sleeps are needed.
"""
from __future__ import annotations

import time
from typing import Optional

import pytest

from cacheforge.app.cache.base import CacheEntry
from cacheforge.app.cache.policies.fifo import FIFOPolicy
from cacheforge.app.cache.policies.lfu import LFUPolicy
from cacheforge.app.cache.policies.lru import LRUPolicy


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_entry(
    key: str,
    created_at: float = 0.0,
    last_accessed: float = 0.0,
    access_count: int = 0,
) -> CacheEntry:
    """Build a minimal CacheEntry with controllable timestamps."""
    return CacheEntry(
        key=key,
        value='{"t":"i","v":1}',
        created_at=created_at,
        last_accessed=last_accessed,
        access_count=access_count,
        size_bytes=64,
    )


# ── LRU ───────────────────────────────────────────────────────────────────────

class TestLRUPolicy:
    def test_evicts_least_recently_used(self):
        policy = LRUPolicy()
        # Insert A at t=1, B at t=2
        policy.on_set(make_entry("A", last_accessed=1.0))
        policy.on_set(make_entry("B", last_accessed=2.0))
        # A should be evicted first (older access)
        assert policy.evict_candidates(1) == ["A"]

    def test_access_moves_to_mru(self):
        policy = LRUPolicy()
        policy.on_set(make_entry("A", last_accessed=1.0))
        policy.on_set(make_entry("B", last_accessed=2.0))
        # Access A — now A is MRU, B should be evicted next
        policy.on_access(make_entry("A", last_accessed=3.0))
        candidates = policy.evict_candidates(1)
        assert candidates == ["B"]

    def test_evict_candidates_order(self):
        policy = LRUPolicy()
        for i, key in enumerate(["A", "B", "C"]):
            policy.on_set(make_entry(key, last_accessed=float(i)))
        # A=t0 is oldest → first eviction candidate
        assert policy.evict_candidates(3) == ["A", "B", "C"]

    def test_on_delete_removes_key(self):
        policy = LRUPolicy()
        policy.on_set(make_entry("X"))
        policy.on_delete("X")
        assert policy.evict_candidates(10) == []

    def test_clear_resets_state(self):
        policy = LRUPolicy()
        policy.on_set(make_entry("A"))
        policy.clear()
        assert policy.evict_candidates(10) == []

    def test_update_does_not_reset_position_incorrectly(self):
        """on_set called twice for same key: it moves to MRU end."""
        policy = LRUPolicy()
        policy.on_set(make_entry("A", last_accessed=1.0))
        policy.on_set(make_entry("B", last_accessed=2.0))
        # Update A (moves it to MRU)
        policy.on_set(make_entry("A", last_accessed=3.0))
        # B (last_accessed=2) is now LRU
        assert policy.evict_candidates(1) == ["B"]

    def test_n_candidates_limited(self):
        policy = LRUPolicy()
        policy.on_set(make_entry("A", last_accessed=1.0))
        policy.on_set(make_entry("B", last_accessed=2.0))
        assert len(policy.evict_candidates(1)) == 1


# ── LFU ───────────────────────────────────────────────────────────────────────

class TestLFUPolicy:
    def test_evicts_least_frequently_used(self):
        policy = LFUPolicy()
        # A accessed 1 time, B 3 times
        policy.on_set(make_entry("A", access_count=1, last_accessed=1.0))
        policy.on_set(make_entry("B", access_count=1, last_accessed=2.0))
        # Access B twice more
        policy.on_access(make_entry("B", last_accessed=3.0))
        policy.on_access(make_entry("B", last_accessed=4.0))
        assert policy.evict_candidates(1) == ["A"]

    def test_tie_break_by_lru(self):
        """Equal frequency: the one accessed LEAST recently goes first."""
        policy = LFUPolicy()
        policy.on_set(make_entry("A", access_count=0, last_accessed=1.0))
        policy.on_set(make_entry("B", access_count=0, last_accessed=2.0))
        # Both at freq=0; A has older last_access → evicted first
        assert policy.evict_candidates(1) == ["A"]

    def test_on_access_increments_count(self):
        policy = LFUPolicy()
        policy.on_set(make_entry("A", access_count=0, last_accessed=1.0))
        policy.on_set(make_entry("B", access_count=0, last_accessed=2.0))
        # Access A → freq 1; B stays at 0 → B is evicted first
        policy.on_access(make_entry("A", last_accessed=3.0))
        assert policy.evict_candidates(1) == ["B"]

    def test_on_delete(self):
        policy = LFUPolicy()
        policy.on_set(make_entry("A"))
        policy.on_delete("A")
        assert policy.evict_candidates(10) == []

    def test_clear(self):
        policy = LFUPolicy()
        policy.on_set(make_entry("A"))
        policy.clear()
        assert policy.evict_candidates(10) == []

    def test_multiple_candidates(self):
        policy = LFUPolicy()
        # freq: C=0, A=1, B=2
        policy.on_set(make_entry("C", access_count=0, last_accessed=1.0))
        policy.on_set(make_entry("A", access_count=0, last_accessed=2.0))
        policy.on_set(make_entry("B", access_count=0, last_accessed=3.0))
        policy.on_access(make_entry("A", last_accessed=4.0))
        policy.on_access(make_entry("B", last_accessed=5.0))
        policy.on_access(make_entry("B", last_accessed=6.0))
        result = policy.evict_candidates(3)
        assert result[0] == "C"   # freq=0
        assert result[1] == "A"   # freq=1
        assert result[2] == "B"   # freq=2


# ── FIFO ──────────────────────────────────────────────────────────────────────

class TestFIFOPolicy:
    def test_evicts_first_inserted(self):
        policy = FIFOPolicy()
        policy.on_set(make_entry("A", created_at=1.0))
        policy.on_set(make_entry("B", created_at=2.0))
        policy.on_set(make_entry("C", created_at=3.0))
        assert policy.evict_candidates(1) == ["A"]

    def test_access_does_not_change_order(self):
        """FIFO ignores access patterns."""
        policy = FIFOPolicy()
        policy.on_set(make_entry("A", created_at=1.0))
        policy.on_set(make_entry("B", created_at=2.0))
        # Heavily access A — order must not change
        for _ in range(10):
            policy.on_access(make_entry("A", last_accessed=99.0))
        assert policy.evict_candidates(1) == ["A"]

    def test_update_keeps_original_insertion_order(self):
        """Updating an existing key must not re-enqueue it at the end."""
        policy = FIFOPolicy()
        policy.on_set(make_entry("A", created_at=1.0))
        policy.on_set(make_entry("B", created_at=2.0))
        # "Update" A with a newer created_at — FIFO should ignore it
        policy.on_set(make_entry("A", created_at=99.0))
        assert policy.evict_candidates(1) == ["A"]

    def test_on_delete(self):
        policy = FIFOPolicy()
        policy.on_set(make_entry("A", created_at=1.0))
        policy.on_delete("A")
        assert policy.evict_candidates(10) == []

    def test_clear(self):
        policy = FIFOPolicy()
        policy.on_set(make_entry("A"))
        policy.clear()
        assert policy.evict_candidates(10) == []

    def test_full_eviction_order(self):
        policy = FIFOPolicy()
        for i, key in enumerate(["X", "Y", "Z"]):
            policy.on_set(make_entry(key, created_at=float(i)))
        assert policy.evict_candidates(3) == ["X", "Y", "Z"]

