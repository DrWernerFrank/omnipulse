"""Runtime configuration. Every field can be set via an ``OMNI_*`` environment variable."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _alias(name: str, *extra: str) -> AliasChoices:
    return AliasChoices(f"OMNI_{name}", *extra)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OMNI_", env_file=".env", extra="ignore")

    # --- topology -----------------------------------------------------------------
    # full: Redis Streams + PostgreSQL/TimescaleDB/pgvector (docker-compose)
    # lite: in-memory bus + store, single process, zero infrastructure
    mode: Literal["full", "lite"] = "full"
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    redis_url: str = "redis://redis:6379/0"
    database_url: str = "postgresql://omni:omni@postgres:5432/omnipulse"
    retention_days: int = 90
    sources_file: str = "config/v1_sources.json"
    shard: str = "0/1"  # "i/n": this engine polls sources where crc32(id) % n == i; shard 0 runs the enricher

    # --- API ----------------------------------------------------------------------
    api_keys: str = ""  # comma-separated; empty = open (local use)
    protect_reads: bool = False
    cors_origins: str = "*"
    public_base_url: str = ""  # externally reachable base URL, enables WebSub push
    static_dir: str = ""  # serve the built terminal from here (auto-detects ../frontend/dist)

    # --- ingestion ----------------------------------------------------------------
    user_agent: str = "Mozilla/5.0 (compatible; OmniPulse/1.0; +https://github.com/omnipulse/omnipulse)"
    sec_user_agent: str = "OmniPulse Research ops@example.com"  # SEC requires "Name email"
    http_timeout: float = 15.0
    max_item_age_hours: float = 24.0  # ignore older items on first poll of a source
    max_backfill_per_source: int = 25
    dlq_failure_threshold: int = 5
    dead_failure_threshold: int = 10

    # --- enrichment ---------------------------------------------------------------
    embedder: Literal["auto", "hash", "fastembed", "openai", "ollama"] = "auto"
    embed_model: str = ""
    embed_dim: int = 384
    cluster_threshold: float | None = None  # None = calibrated per embedder (0.88 neural)
    cluster_window_hours: float = 24.0
    cluster_index_capacity: int = 60000
    simhash_max_distance: int = 3
    fetch_full_article: bool = True
    full_article_min_urgency: int = 5
    full_article_concurrency: int = 4

    llm_provider: Literal["none", "openai", "anthropic", "ollama", "openai_compatible"] = "none"
    llm_model: str = ""
    llm_base_url: str = ""  # openai_compatible: any /v1/chat/completions endpoint (env-only, never committed)
    llm_api_key: str = Field("", validation_alias=_alias("LLM_API_KEY", "LLM_API_KEY"))
    llm_min_urgency: int = 7
    llm_concurrency: int = 4
    llm_max_per_minute: int = 30

    # --- provider credentials (all optional) --------------------------------------
    openai_api_key: str = Field("", validation_alias=_alias("OPENAI_API_KEY", "OPENAI_API_KEY"))
    anthropic_api_key: str = Field("", validation_alias=_alias("ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"))
    ollama_url: str = Field("http://ollama:11434", validation_alias=_alias("OLLAMA_URL", "OLLAMA_URL"))
    newsapi_key: str = Field("", validation_alias=_alias("NEWSAPI_KEY", "NEWSAPI_KEY"))
    x_bearer_token: str = Field("", validation_alias=_alias("X_BEARER_TOKEN", "X_BEARER_TOKEN"))
    whale_alert_key: str = Field("", validation_alias=_alias("WHALE_ALERT_KEY", "WHALE_ALERT_KEY"))

    # --- memory bounds (lite mode + fan-out) --------------------------------------
    memory_max_events: int = 50000
    subscriber_queue_size: int = 5000

    @property
    def api_key_set(self) -> set[str]:
        return {k.strip() for k in self.api_keys.split(",") if k.strip()}

    def credential(self, name: str) -> str:
        return str(getattr(self, name, "") or "")


@lru_cache
def get_settings() -> Settings:
    return Settings()
