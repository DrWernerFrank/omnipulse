"""Application settings. Every value overridable via OMNIPULSE_* env vars or .env."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OMNIPULSE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- core ----
    env: str = "production"
    log_level: str = "INFO"
    api_key: str | None = None
    public_url: str = "http://localhost:8000"
    sources_file: str = "config/default_sources.json"
    api_url: str = "http://localhost:8000"  # used by the MCP server to reach the REST API

    # ---- infrastructure (None => built-in in-process fallback, system still fully functional) ----
    database_url: str | None = None
    redis_url: str | None = None

    # ---- http ----
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000,http://localhost:5173"
    user_agent: str = (
        "OmniPulse/1.0 (+news aggregation terminal; contact: ops@omnipulse.local)"
    )
    fetch_timeout_sec: float = 20.0
    per_host_rate: float = 2.0   # sustained req/sec per host
    per_host_burst: int = 6       # token bucket capacity per host
    http2: bool = True            # ALPN-negotiated; falls back to h1.1 automatically
    keepalive_expiry_sec: float = 300.0  # keep warm connections across poll cycles
                                           # (default 5s forces a TLS handshake every poll)

    # ---- ingestion ----
    ingest_concurrency: int = 64
    poll_tick_sec: float = 0.1    # scheduler wake-up granularity
    stats_interval_sec: float = 1.0
    max_poll_sec_cap: int = 1800
    auto_pause_failures: int = 8
    resume_probe_sec: int = 900
    max_item_age_hours: int = 24

    # ---- enrichment ----
    embedding_dim: int = 256
    cluster_similarity: float = 0.88
    cluster_window_hours: int = 24
    cluster_recency_hours: int = 24    # only match against clusters touched this recently
    cluster_candidate_cap: int = 600    # max candidates compared per event (numpy path)
    cluster_fallback_cap: int = 150     # pure-python path cap when numpy is absent
    dedup_hamming: int = 3
    fulltext_enabled: bool = True
    fulltext_max_chars: int = 6000

    # ---- websub push (instant delivery; needs a publicly reachable public_url) ----
    websub_enabled: bool = False
    websub_lease_sec: int = 86400
    websub_renew_sec: int = 21600

    # ---- optional LLM enrichment (heuristic fallback when absent) ----
    llm_provider: str = "auto"  # auto|none|openai|anthropic|ollama
    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"
    anthropic_api_key: str | None = None
    anthropic_model: str = "claude-haiku-4.5"
    ollama_url: str | None = None
    ollama_model: str = "llama3.1:8b"

    # ---- optional keyed source connectors ----
    newsapi_key: str | None = None
    whale_alert_key: str | None = None
    telegram_api_id: int | None = None
    telegram_api_hash: str | None = None

    # ---- bus / buffers ----
    stream_maxlen: int = 200_000
    memory_buffer: int = 20_000

    @property
    def cors_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
