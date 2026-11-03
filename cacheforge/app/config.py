"""CacheForge application configuration.

All settings are read from environment variables (or a .env file loaded by
python-dotenv).  Pydantic-settings handles coercion and validation.
"""
from __future__ import annotations

import os
from enum import Enum
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EvictionPolicyEnum(str, Enum):
    lru = "lru"
    lfu = "lfu"
    fifo = "fifo"


class Settings(BaseSettings):
    """Application-wide settings, sourced from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Identity ───────────────────────────────────────────────────────────
    instance_id: str = Field(default="local", description="Unique name of this API instance.")

    # ── Database ───────────────────────────────────────────────────────────
    postgres_user: str = "cacheforge"
    postgres_password: str = "cacheforge"
    postgres_db: str = "cacheforge"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    @property
    def database_url(self) -> str:
        """Async DSN for asyncpg / SQLAlchemy async."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_plain(self) -> str:
        """Plain asyncpg DSN (without SQLAlchemy driver prefix)."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # ── Redis ──────────────────────────────────────────────────────────────
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL.",
    )

    # ── Cache behaviour ────────────────────────────────────────────────────
    cache_enabled: bool = Field(
        default=True,
        description="Master switch; when False the cache is bypassed entirely.",
    )
    cache_namespace: str = Field(
        default="cf",
        description="Top-level key prefix applied to every cache entry.",
    )
    cache_default_ttl: int = Field(
        default=300,
        ge=1,
        description="Default TTL in seconds for cached entries.",
    )
    cache_negative_ttl: int = Field(
        default=30,
        ge=1,
        description="TTL in seconds for negative (not-found) cache entries.",
    )
    cache_ttl_jitter: int = Field(
        default=10,
        ge=0,
        description="Max random jitter in seconds added to TTL to avoid mass expiry.",
    )
    cache_max_entries: int = Field(
        default=10_000,
        ge=1,
        description="Maximum number of entries in the cache.",
    )
    cache_max_memory_bytes: int = Field(
        default=100 * 1024 * 1024,  # 100 MB
        ge=1,
        description="Soft memory limit in bytes; entries are evicted when exceeded.",
    )
    cache_eviction_policy: EvictionPolicyEnum = Field(
        default=EvictionPolicyEnum.lru,
        description="Eviction policy: lru | lfu | fifo.",
    )

    # ── Stampede protection ────────────────────────────────────────────────
    cache_stampede_timeout: float = Field(
        default=5.0,
        ge=0.1,
        description="Seconds a waiter will wait for the leader to populate the cache before falling back to a direct DB read.",
    )
    cache_lock_ttl: int = Field(
        default=10_000,
        ge=100,
        description="Redis distributed lock TTL in milliseconds.",
    )

    # ── App ────────────────────────────────────────────────────────────────
    log_level: str = Field(default="INFO", description="Python logging level.")


# Module-level singleton — import this everywhere.
settings = Settings()

